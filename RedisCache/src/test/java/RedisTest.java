import edu.yu.mdm.RedisCache;
import edu.yu.mdm.RedisCacheBase;
import static edu.yu.mdm.RedisCacheBase.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class RedisTest {

    private RedisCacheBase cache;

    @BeforeEach
    public void setUpDatabase() {
        cache = new RedisCache(new Jedis());
    }

    @Test
    public void checkCookieNull() {
        assertNull(cache.checkCookie("this is not a cookie"));
    }

    @Test
    public void updateCookieThrows() {
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie(null, "Hello", "whatever"));
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie("", "Hello", "whatever"));
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie("Hello", null, "whatever"));
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie("Hello", "", "whatever"));
    }

    @Test
    public void checkCookies() {
        cache.updateCookie("Oreo", "Nabisco", null);
        cache.updateCookie("Chips", "Company", null);
        cache.updateCookie("Oreo", "Nabisco", null);
        cache.updateCookie("Chips", "Company", "Assembly line");
        assertEquals("Nabisco", cache.checkCookie("Oreo"));
        assertEquals("Company", cache.checkCookie("Chips"));
        cache.updateCookie("Flying Saucer", "Carvel", "Moon");
        assertEquals("Carvel", cache.checkCookie("Flying Saucer"));
    }

    // To test getItems:
    // Check that adding one item changes it.
    @Test
    public void addOneItem() {
        cache.updateCookie("Oreo", "Nabisco", "cream");
        String[] result = {"cream"};
        assertArrayEquals(result, cache.getItems("Oreo"));
    }

    // Check that adding nothing is still an empty array
    @Test
    public void addNoItems() {
        assertArrayEquals(new String[0], cache.getItems("Oreo"));
    }

    // Check that adding a null doesn't, whether empty or not
    @Test
    public void addNullItem() {
        cache.updateCookie("Oreo", "Nabisco", null);
        assertArrayEquals(new String[0], cache.getItems("Oreo"));
    }

    // Check that we get the right order with more than one
    @Test
    public void addItemsInOrder() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Oreo", "Nabisco", "cream");
        String[] result = {"cookie", "cream"};

        cache.updateCookie("CookiesNCream", "Leibish", "cream");
        cache.updateCookie("CookiesNCream", "Leibish", "cookie");
        String[] result2 = {"cream", "cookie"};

        assertArrayEquals(result, cache.getItems("Oreo"));
        assertArrayEquals(result2, cache.getItems("CookiesNCream"));
    }

    // Check that a more complex series is fine
    @Test
    public void addManyItems() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Oreo", "Nabisco", "cream");
        cache.updateCookie("CookiesNCream", "Leibish", "cream");
        cache.updateCookie("Oreo", "Nabisco", "golden");

        String[] result = {"cookie", "cream", "golden"};
        String[] result2 = {"cream"};

        assertArrayEquals(result, cache.getItems("Oreo"));
        assertArrayEquals(result2, cache.getItems("CookiesNCream"));
    }

    // Check that if you add more than the limit, it overwhelms
    @Test
    public void addMoreItemsThanLimit() {
        int limit = RedisCacheBase.MAX_ITEMS_CACHED_PER_USER;
        String[] result = new String[limit];
        // since we are going <= limit, we should have one over
        for (int i = 0; i <= limit; i++) {
            cache.updateCookie("Oreo", "Nabisco", "variant" + i);
            if (i != 0) result[i - 1] = "variant" + i;
        }

        assertArrayEquals(result, cache.getItems("Oreo"));
    }

    // for getCookies:
    // check that if there are no cookies, it returns null
    @Test
    public void addNoCookie() {
        assertArrayEquals(new String[0], cache.getCookies());
    }

    // check that if there is one cookie, it returns that
    @Test
    public void checkOneCookie() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        String[] result = {"Oreo"};
        assertArrayEquals(result, cache.getCookies());
    }

    // check that it returns a list of many when there are many
    @Test
    public void checkManyCookies() {
        cache.updateCookie("Oreo", "Nabisco", null);
        cache.updateCookie("Chips", "Company", null);
        String[] result1 = {"Oreo", "Chips"};
        //System.out.println(Arrays.toString(result1));
        assertArrayEquals(result1, cache.getCookies());

        cache.updateCookie("Flying Saucer", "Carvel", "Moon");
        String[] result2 = {"Oreo", "Chips", "Flying Saucer"};
        assertArrayEquals(result2, cache.getCookies());
    }

    // check that if a cookie is viewed, its position in the array changes
    @Test
    public void checkViewingCookieChangesPosition() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Chips", "Company", null);
        cache.updateCookie("Flying Saucer", "Carvel", "Moon");
        String[] result1 = {"Oreo", "Chips", "Flying Saucer"};
        assertArrayEquals(result1, cache.getCookies());

        cache.updateCookie("Oreo", "Nabisco", "cream");
        String[] result2 = {"Chips", "Flying Saucer", "Oreo"};
        assertArrayEquals(result2, cache.getCookies());
    }

    // for the cart methods:
    // check that if you add no items, the cart is empty
    @Test
    public void noItemsNoCart() {
        assertEquals(Collections.emptyMap(), cache.getCart("Oreo"));
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        assertEquals(Collections.emptyMap(), cache.getCart("Oreo"));
    }
    // check that if you add an item, it's in the cart
    @Test
    public void oneItemCart() {
        cache.modifyCart("Oreo", "12-pack", 5);
        assertEquals(Map.of("12-pack", 5), cache.getCart("Oreo"));
    }

    // like before, but with more items
    @Test
    public void multiItemCart() {
        cache.modifyCart("Oreo", "12-pack", 5);
        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Chips", "6-pack", 2);
        assertEquals(Map.of("12-pack", 5, "6-pack", 10), cache.getCart("Oreo"));
        cache.modifyCart("Oreo", "48-pack", 15);
        assertEquals(Map.of("12-pack", 5, "6-pack", 10, "48-pack", 15), cache.getCart("Oreo"));
        assertEquals(Map.of("6-pack", 2), cache.getCart("Chips"));
    }

    // check that you can change the amount in an item, both positively and negatively
    @Test
    public void itemChangingQuantityCart() {
        cache.modifyCart("Oreo", "eggs", 6);
        assertEquals(Map.of("eggs", 6), cache.getCart("Oreo"));
        // That's too expensive!
        cache.modifyCart("Oreo", "eggs", 3);
        assertEquals(Map.of("eggs", 3), cache.getCart("Oreo"));
        // Gaston was killed, his consumption is gone, price of eggs plummets
        cache.modifyCart("Oreo", "eggs", 12);
        assertEquals(Map.of("eggs", 12), cache.getCart("Oreo"));
    }

    // check that you can remove items with - and 0
    @Test
    public void removeItemsFromCart() {
        cache.modifyCart("Oreo", "12-pack", 5);
        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Oreo", "6-pack", 0);
        assertEquals(Map.of("12-pack", 5), cache.getCart("Oreo"));
        cache.modifyCart("Oreo", "12-pack", -1);
        assertEquals(Map.of(), cache.getCart("Oreo"));
        cache.modifyCart("Oreo", "6-pack", 10);
        assertEquals(Map.of("6-pack", 10), cache.getCart("Oreo"));
        cache.modifyCart("Oreo", "6-pack", -10);
        assertEquals(Map.of(), cache.getCart("Oreo"));
    }

    // the following tests are for what happens if a viewer views the same item more than once
    // check that if he views it twice, it only appears once
    @Test
    public void addItemTwice() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        String[] result = {"cookie"};
        assertArrayEquals(result, cache.getItems("Oreo"));
    }

    // check that if he views it twice with something in the middle, it changes the order
    @Test
    public void addItemTwiceWithObstruction() {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Oreo", "Nabisco", "cream");
        String[] result = {"cookie", "cream"};
        assertArrayEquals(result, cache.getItems("Oreo"));

        cache.updateCookie("Oreo", "Nabisco", "cookie");
        String[] result2 = {"cream", "cookie"};
        assertArrayEquals(result2, cache.getItems("Oreo"));
    }

    // check that if he views it and then adds something beyond the limit, it doesn't remove what he just used
    @Test
    public void addMoreItemsThanLimitAndView() {
        int limit = RedisCacheBase.MAX_ITEMS_CACHED_PER_USER;
        String[] result = new String[limit];
        for (int i = 0; i < limit; i++) {
            cache.updateCookie("Oreo", "Nabisco", "variant" + i);
            if (i > 1) result[i - 2] = "variant" + i;
        }
        cache.updateCookie("Oreo", "Nabisco", "variant0");
        cache.updateCookie("Oreo", "Nabisco", "variant" + limit);
        result[limit - 2] = "variant0";
        result[limit - 1] = "variant" + limit;

        assertArrayEquals(result, cache.getItems("Oreo"));
    }

    // cleaner thread tests
    // note that for these, I assume that every key has a user
    // if a key didn't have a user, its stuff would never be deleted
    // check that if there is nothing added, the cleaner thread does nothing
    @Test
    public void noCookieCleaner() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(4));
        int numDeleted = cleanerThread.get();

        assertArrayEquals(new String[0], cache.getCookies());
        assertEquals(0, numDeleted);
    }

    // check that if we have less than the limit, the cleaner thread does nothing
    @Test
    public void atLimitCleaner() throws ExecutionException, InterruptedException {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Chips", "Company", null);
        cache.modifyCart("Oreo", "6-pack", 10);

        String[] cookiesResult = {"Oreo", "Chips"};
        String[] itemsResult = {"cookie"};

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(2));
        int numDeleted = cleanerThread.get();

        assertEquals("Nabisco", cache.checkCookie("Oreo"));
        assertArrayEquals(cookiesResult, cache.getCookies());
        assertArrayEquals(itemsResult, cache.getItems("Oreo"));
        assertEquals(Map.of("6-pack", 10), cache.getCart("Oreo"));
        assertEquals(0, numDeleted);
    }

    // check that if we have more than the limit, the cleaner thread gets rid of the oldest thing
        // check that its stuff is also deleted
    @Test
    public void aboveLimitCleaner() throws ExecutionException, InterruptedException {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Chips", "Company", "chocolate");
        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Chips", "6-pack", 5);

        String[] cookiesResult = {"Chips"};
        String[] itemsResult = {"chocolate"};

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(1));
        int numDeleted = cleanerThread.get();

        assertArrayEquals(cookiesResult, cache.getCookies());

        assertNull(cache.checkCookie("Oreo"));
        assertArrayEquals(new String[0], cache.getItems("Oreo"));
        assertEquals(Map.of(), cache.getCart("Oreo"));

        assertEquals("Company", cache.checkCookie("Chips"));
        assertArrayEquals(itemsResult, cache.getItems("Chips"));
        assertEquals(Map.of("6-pack", 5), cache.getCart("Chips"));

        assertEquals(1, numDeleted);
    }

    // check that the oldest thing changes based on repeat updateCookie uses
    @Test
    public void cleanerRespectsUpdates() throws ExecutionException, InterruptedException {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Chips", "Company", "chocolate");
        cache.updateCookie("Oreo", "Nabisco", "cream");
        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Chips", "6-pack", 5);

        String[] cookiesResult = {"Oreo"};
        String[] itemsResult = {"cookie", "cream"};

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(1));
        int numDeleted = cleanerThread.get();

        assertArrayEquals(cookiesResult, cache.getCookies());

        assertEquals("Nabisco", cache.checkCookie("Oreo"));
        assertArrayEquals(itemsResult, cache.getItems("Oreo"));
        assertEquals(Map.of("6-pack", 10), cache.getCart("Oreo"));

        assertNull(cache.checkCookie("Chips"));
        assertArrayEquals(new String[0], cache.getItems("Chips"));
        assertEquals(Map.of(), cache.getCart("Chips"));

        assertEquals(1, numDeleted);
    }

    // check that the cleaner thread removes more than one thing
    @Test
    public void cleanerDeletesMoreThanOne() throws ExecutionException, InterruptedException {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Flying Saucer", "Carvel", "Moon");
        cache.updateCookie("Chips", "Company", "chocolate");
        cache.updateCookie("CookiesNCream", "Leibish", null);

        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Flying Saucer", "Ice Cream Cake!", 1);
        cache.modifyCart("Chips", "6-pack", 5);

        String[] cookiesResult = {"Chips", "CookiesNCream"};
        String[] itemsResult = {"chocolate"};

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(2));
        int numDeleted = cleanerThread.get();

        assertArrayEquals(cookiesResult, cache.getCookies());

        assertNull(cache.checkCookie("Oreo"));
        assertArrayEquals(new String[0], cache.getItems("Oreo"));
        assertEquals(Map.of(), cache.getCart("Oreo"));

        assertNull(cache.checkCookie("Flying Saucer"));
        assertArrayEquals(new String[0], cache.getItems("Flying Saucer"));
        assertEquals(Map.of(), cache.getCart("Flying Saucer"));

        assertEquals("Company", cache.checkCookie("Chips"));
        assertArrayEquals(itemsResult, cache.getItems("Chips"));
        assertEquals(Map.of("6-pack", 5), cache.getCart("Chips"));

        assertEquals("Leibish", cache.checkCookie("CookiesNCream"));

        assertEquals(2, numDeleted);
    }

    // check that if the limit is 0, we remove everything
    @Test
    public void limit0CleansAll() throws ExecutionException, InterruptedException {
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        cache.updateCookie("Chips", "Company", "chocolate");
        cache.modifyCart("Oreo", "6-pack", 10);
        cache.modifyCart("Chips", "6-pack", 5);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> cleanerThread = executor.submit(new RedisCache.CleanerThread(0));
        int numDeleted = cleanerThread.get();

        assertArrayEquals(new String[0], cache.getCookies());

        assertNull(cache.checkCookie("Oreo"));
        assertArrayEquals(new String[0], cache.getItems("Oreo"));
        assertEquals(Map.of(), cache.getCart("Oreo"));

        assertNull(cache.checkCookie("Chips"));
        assertArrayEquals(new String[0], cache.getItems("Chips"));
        assertEquals(Map.of(), cache.getCart("Chips"));


        assertEquals(2, numDeleted);
    }

    // sanity check - make sure his code runs
    @Test
    public void sanityCheck() {
        final int limit = 0;
        CleanerThreadBase task = new RedisCache.CleanerThread(limit);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Integer> future = executorService.submit(task);
    }

    // I just realized that I need to make sure that it throws an error if we try to update item with a user who isn't the cookie's user
    @Test
    public void differentUserThrows() {
        cache.updateCookie("Oreo", "Nabisco", null);
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie("Oreo", "Sugar Rush", null));
        cache.updateCookie("Oreo", "Nabisco", "cookie");
        assertThrows(IllegalArgumentException.class, ()->cache.updateCookie("Oreo", "Sugar Rush", "guard"));
    }

}
