package cn.shijinshi.redis.replicate;

/**
 * @author Gui Jiahai
 */
public interface Observer<T> {

    void onNext(T obj);

    void onCompleted(T obj);

    void onError(T obj);

}
