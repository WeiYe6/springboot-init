package com.fengye.springbootinit.job.once;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fengye.springbootinit.model.entity.User;
import com.fengye.springbootinit.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 缓存预热 定时任务 （add 分布式锁）
 * author: fengye
 */
//@Component  不执行(按需)
@Slf4j
public class PreCacheJob {

    @Resource
    private UserService userService;
    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    @Resource
    private RedissonClient redissonClient;

    //定义重点用户，来进行预热
    private final List<Long> mainUserList = Arrays.asList(1L, 2L);

    //每天在指定的时间，执行预热,这里采用给重点用户的推荐页面进行缓存预热 （这里：给用户id为1和id为2的用户进行预热）
    @Scheduled(cron = "0 10 10 * * *")  //cron表达式 ---设置触发时间
    public void doCacheRecommendUser() {
        //获取分布式锁对象,并设置键
        RLock lock = redissonClient.getLock("fengye:precachejob:docache:lock");
        try {
            //尝试获取锁 只允许某台服务器中的某个线程能抢到锁 （等待时长，过期时间（-1 触发续期），时间单位）
            if (lock.tryLock(0, -1, TimeUnit.MILLISECONDS)) {
                System.out.println("getLock:--" + Thread.currentThread().getId());
                //查数据库
                QueryWrapper<User> queryWrapper = new QueryWrapper<>();
                Page<User> userPage = userService.page(new Page<>(1, 20), queryWrapper);
                //添加进缓存中，注意一定要设置过期时间(缓存数据的过期)，我这里设置为3600秒 （一个小时）
                String redisKey = String.format("xxxx:user:recommend:%s", mainUserList);
                ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
                try {
                    valueOperations.set(redisKey, userPage, 3600000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.error("redis set key error,", e);
                }
            }
        } catch (InterruptedException e) {
            log.error("doCacheRecommendUser error", e);
        } finally {
            //释放锁（只能释放自己的锁）
            if (lock.isHeldByCurrentThread()) {
                System.out.println("unlock:--" + Thread.currentThread().getId());
                lock.unlock();
            }
        }

    }

}
