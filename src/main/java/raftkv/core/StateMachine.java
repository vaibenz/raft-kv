package raftkv.core;

/** The application that committed commands are applied to, in log order, exactly once per index. */
public interface StateMachine {
    byte[] apply(long index, byte[] command);
}
