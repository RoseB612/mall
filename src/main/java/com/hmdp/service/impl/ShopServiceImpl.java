package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// Sentinel Big Tech Imports
import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;

import javax.annotation.Generated;
import javax.annotation.Resource;
import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient clientClient;

    @Resource
    private RedissonClient redissonClient;

    // 核心引流站：布隆过滤器
    private RBloomFilter<Long> shopBloomFilter;

    /**
     * 第一道城墙：项目启动时，捞出全部物理数据灌进布隆过滤器
     */
    @PostConstruct
    public void initBloomFilter() {
        // 创建 Redisson 的布隆代理对象
        shopBloomFilter = redissonClient.getBloomFilter("shop:bloom:filter");
        // 初始化。预计未来存放上十万级别以上的商品 ID，允许的误判率为百分之一 (0.01)。
        // 哪怕十万级的数据量，在布隆过滤器的位图也仅仅消耗不到 200KB 的惊人微小极低内存！
        shopBloomFilter.tryInit(100000L, 0.01);

        // 【实战点】把目前数据库里真正存在的所有商铺 ID 灌输进去预热
        List<Shop> allShops = this.list();
        for (Shop s : allShops) {
            shopBloomFilter.add(s.getId());
        }
        System.out.println("【大厂布隆防线预装完成】成功从底层库捞取并装入布隆过滤器 " + allShops.size() + " 个法定商铺ID！");
    }

    // 重写新增方法：每次商家发布新店，必须手工把它塞进布隆里，否则别人死活搜不到
    @Override
    public boolean save(Shop entity) {
        boolean saved = super.save(entity);
        if (saved && entity.getId() != null) {
            shopBloomFilter.add(entity.getId());
        }
        return saved;
    }

    @Override
    @SentinelResource(value = "queryShopById", blockHandler = "handleQueryShopBlock")
    public Result queryById(Long id) {
        // --- 方案零（防穿透绝对防御）：前置布隆过滤器拦截 ---
        if (!shopBloomFilter.contains(id)) {
            System.out.println("【大厂防黑客】布隆防线判定该ID绝不存在，毫无内存损耗微秒斩杀穿透请求！恶意假ID：" + id);
            return Result.fail("您查询的商铺压根不存在！(布隆防盲扫命中)");
        }

        // --- 第二级防御：查 Redis（这里走逻辑过期方案解决击穿） ---
        Shop shop = clientClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL,
                TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        return Result.ok(shop);
    }

    // --- 方案三落地：Sentinel 限流超载的降级方法 ---
    public Result handleQueryShopBlock(Long id, BlockException ex) {
        System.out.println("【大厂高可用】Sentinel 拉闸限流生效，保护了 MySQL 不被大面积击穿！");
        return Result.fail("当前太火爆啦挤不进去，请稍后再来试试看！(Sentinel限流降级)");
    }

    // 延迟双删专用线程池（单线程即可，任务量极小）
    private static final ScheduledExecutorService DELAY_DELETE_EXECUTOR = Executors.newScheduledThreadPool(1);

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /*
     * public Shop queryWithLogicalExpire(Long id) {
     * // //1.尝试从Redis查询商铺缓存
     * // String shopJson =
     * stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
     * // //2.判断缓存是否存在
     * // if(StrUtil.isBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
     * // //3.不存在，返回商铺信息
     * // return null;
     * //
     * // }
     * //
     * // //4.存在，将json反序列化为对象
     * // RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
     * // Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
     * // LocalDateTime expireTime = redisData.getExpireTime();
     * // //5.判断是否过期
     * // if(expireTime.isAfter(LocalDateTime.now())) {
     * // //5.1.未过期，直接返回店铺信息
     * // return shop;
     * // }
     * // //5.2.已过期，需要返回缓存重建
     * // //6.缓存重建
     * // //6.1.获取互斥锁
     * // String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
     * // boolean isLock = tryLock(lockKey);
     * // //6.2.判断是否获取锁成功
     * // if(isLock){
     * // // 6.3.成功，开启独立线程实现缓存重建
     * // CACHE_REBUILD_EXECUTOR.submit(()->{
     * // try {
     * // //重建缓存
     * // this.saveShop2Redis(id,20L);
     * // } catch (Exception e) {
     * // throw new RuntimeException(e);
     * // }finally {
     * // //释放锁
     * // unLock(lockKey);
     * // }
     * // });
     * //
     * // }
     * //
     * // //6.4.返回过期的商铺信息
     * // return shop;
     * //
     * // }
     * /**
     * 互斥锁解决缓存穿透
     * 
     * @param id
     * 
     * @return
     * 
     * @throws InterruptedException
     */
    /*
     * public Shop queryWithMutex(Long id) throws InterruptedException {
     * //1.尝试从Redis查询商铺缓存
     * String shopJson =
     * stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
     * //2.判断缓存是否存在
     * if(StrUtil.isNotBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
     * //3.存在，返回商铺信息
     * return JSONUtil.toBean(shopJson, Shop.class);
     * 
     * }
     * //判断是否为空值
     * if(shopJson!=null){
     * return null;
     * }
     * //4.实现缓存重建
     * //4.1获取互斥锁
     * String lockKey="lock:shop:"+id;
     * Shop shop=null;
     * try {
     * boolean isLock = tryLock(lockKey);
     * //4.2判断是否获取成功
     * if(!isLock) {
     * //4.3失败，则休眠重试
     * Thread.sleep(50);
     * return queryWithMutex(id);
     * }
     * 
     * //4.4.成功，根据id查询数据库
     * shop = getById(id);
     * //模拟重建的延迟
     * Thread.sleep(200);
     * //5.判断数据库中是否存在
     * if(shop==null){
     * //6.不存在，返回错误状态码
     * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
     * RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
     * return null;
     * }
     * //7.存在，写入redis，返回商铺信息
     * String newShopJson = JSONUtil.toJsonStr(shop);
     * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,
     * newShopJson,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
     * } catch (InterruptedException e) {
     * throw new RuntimeException(e);
     * }finally {
     * unLock(lockKey);
     * }
     * 
     * //9.返回
     * return shop;
     * 
     * }
     */
    /**
     * 缓存穿透
     * 
     * @param id
     * @return
     */
    /*
     * // //1.尝试从Redis查询商铺缓存
     * // String shopJson =
     * stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
     * // //2.判断缓存是否存在
     * // if(StrUtil.isNotBlank(shopJson)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
     * // //3.存在，返回商铺信息
     * // return JSONUtil.toBean(shopJson, Shop.class);
     * //
     * // }
     * // //判断是否为空值
     * // if(shopJson!=null){
     * // return null;
     * // }
     * // //4.不存在，根据id查询数据库
     * // Shop shop = getById(id);
     * // //5.判断数据库中是否存在
     * // if(shop==null){
     * // //6.不存在，返回错误状态码
     * // stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
     * RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
     * // return null;
     * // }
     * // //7.存在，写入redis，返回商铺信息
     * // String newShopJson = JSONUtil.toJsonStr(shop);
     * // stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,
     * newShopJson,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
     * //
     * // return shop;
     * //
     * // }
     * 
     */

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.封装成逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        
        // ========== Canal 异步解耦改造 ==========
        // 之前这里是"延迟双删"代码。
        // 现在我们只负责更新数据库。
        // 更新成功后，MySQL 会产生 binlog。
        // Canal Server 监听到 binlog 后，会推送给我们的 CanalBinlogListener。
        // 由 CanalBinlogListener 负责异步且可靠地删除 Redis 缓存。
        // 彻底解耦了业务逻辑和缓存一致性逻辑！
        updateById(shop);

        return Result.ok();
    }


    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.是否根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，该数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis，按照距离排序，分页。结果：shopId,distance
        String key = SHOP_GEO_KEY + typeId;
        // 在 Redis 中按地理坐标（x, y）查询距离当前用户位置 5000 米内的商店
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        // 4.解析出id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取从from到end部分 跳过前 from 个结果，实现分页
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
