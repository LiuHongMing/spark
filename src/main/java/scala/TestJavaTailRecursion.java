package scala;

public class TestJavaTailRecursion {

    private long sum(long n, long total) {
        if (n <= 0) {
            return total;
        }
        return sum(n - 1, total + n);
    }

    public static void main(String[] args) {
        TestJavaTailRecursion ttr = new TestJavaTailRecursion();
        long sum = ttr.sum(100000, 0);
        System.out.println(sum);
    }

}
