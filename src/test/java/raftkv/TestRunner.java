package raftkv;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Runs every {@link Test} method in the listed classes. Pass class names to run a subset. */
public final class TestRunner {
    private static final List<Class<?>> ALL = List.of(CodecTest.class, KvStateMachineTest.class,
            FileStorageTest.class, raftkv.sim.LinearizabilityTest.class, raftkv.sim.StorageCrashTest.class,
            raftkv.lsm.LsmStoreTest.class, raftkv.sim.LsmCrashTest.class,
            RaftTest.class, ChaosTest.class, TcpClusterTest.class, raftkv.sim.SimTest.class);

    public static void main(String[] args) throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        if (args.length == 0) classes.addAll(ALL);
        else for (String a : args) classes.add(Class.forName("raftkv." + a));

        int passed = 0, failed = 0;
        long start = System.currentTimeMillis();
        for (Class<?> c : classes) {
            Method[] methods = c.getDeclaredMethods();
            Arrays.sort(methods, Comparator.comparing(Method::getName));
            for (Method m : methods) {
                if (!m.isAnnotationPresent(Test.class)) continue;
                long t0 = System.currentTimeMillis();
                String name = c.getSimpleName() + "." + m.getName();
                try {
                    m.invoke(c.getDeclaredConstructor().newInstance());
                    passed++;
                    System.out.printf("  PASS  %-55s %6d ms%n", name, System.currentTimeMillis() - t0);
                } catch (InvocationTargetException e) {
                    failed++;
                    System.out.printf("  FAIL  %-55s %6d ms%n", name, System.currentTimeMillis() - t0);
                    e.getCause().printStackTrace(System.out);
                }
            }
        }
        System.out.printf("%n%d passed, %d failed in %.1f s%n", passed, failed,
                (System.currentTimeMillis() - start) / 1000.0);
        System.exit(failed == 0 ? 0 : 1);
    }
}
