package com.hmdp.config;

import com.alibaba.csp.sentinel.annotation.aspectj.SentinelResourceAspect;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * 注入 Sentinel 的切面拦截器，使得 @SentinelResource 注解生效
 */
@Configuration
public class SentinelConfig {

    @Bean
    public SentinelResourceAspect sentinelResourceAspect() {
        return new SentinelResourceAspect();
    }

    /**
     * 为了方便你在本地无缝体验限流效果，这里用代码直接写死一条限流规则
     * 规则：对 queryShopById 这个资源，每秒钟最多允许 2 个请求通过
     * 测试方式：在浏览器里不停狂按 F5 刷新商铺信息，由于人手肯定能按出 1秒3次，必然触发限流降级
     */
    @PostConstruct
    private void initSentinelRules() {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule rule = new FlowRule();
        rule.setResource("queryShopById"); // 保护的资源名（必须和 @SentinelResource 的名字一样）
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS); // 基于 QPS（每秒并发数）限流
        rule.setCount(2); // 极端设定：一秒钟只准通过 2 次请求！
        rules.add(rule);

        FlowRuleManager.loadRules(rules);
        System.out.println("【大厂高可用系统】Sentinel 限流防波堤部署完毕！商铺查询 QPS 最高阀值：2");
    }
}
