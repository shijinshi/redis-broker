package cn.shijinshi.redis.control.rpc;

import java.io.Serializable;

/**
 * @author Gui Jiahai
 */
public class Answer implements Serializable {

    private State state;
    private String message;

    public static Answer ok() {
        return new Answer().setState(State.OK);
    }

    public static Answer expire() {
        return new Answer().setState(State.EXPIRE);
    }

    public static Answer bad(String message) {
        return new Answer().setState(State.BAD)
                .setMessage(message);
    }

    public static Answer next() {
        return new Answer().setState(State.NEXT);
    }

    public Answer() {
    }

    public State getState() {
        return state;
    }

    public Answer setState(State state) {
        this.state = state;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Answer setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "Answer{" +
                "state=" + state +
                ", message='" + message + '\'' +
                '}';
    }
}
