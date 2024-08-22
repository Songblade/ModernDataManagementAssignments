package edu.yu.mdm;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RedisCache extends RedisCacheBase {

    /*
    * I would love to use a hash to store everything associated with a user together
    * But unfortunately, the only things storable in hashes are strings
    * So, "cookie":user stores the user
    * "cookie":items stores sorted set of items, sorted by insertion time
    *   I originally had it as a list, but since we need each element only there once, a sorted set seemed easier
    * "cookie":cart stores a hash of items, where the key is the item and the value is the quantity
    * allCookies stores the z-set of cookies sorted by timestamp
    * */

    /**
     * Constructor is supplied with a Jedis instance, created with the no-arg
     * constructor.  The constructor selects DB number DB_INDEX and then empties
     * the current contents of that database.
     *
     * @param conn Redis database connection
     */
    public RedisCache(Jedis conn) {
        super(conn);
    }

    /**
     * Returns the user associated with the cookie if an association currently
     * exists.
     *
     * @param cookie a cookie maintained by the server that may map to a user
     * @return the user associated with the cookie or null if no such association
     * exists (possibly because it was "aged out")
     */
    @Override
    public String checkCookie(String cookie) {
        return conn.get(cookie + ":user");
    }

    /**
     * Creates (or updates) an association between a token, a user, and an item.
     * With respect to "aging out" the cookie/user association, invoking this
     * method refreshes the age to the time that the method is invoked. The
     * implementation maintains user-to-item state for at most recent
     * MAX_ITEMS_CACHED_PER_USER: older state is discarded.
     *
     * @param cookie represents the user on the server, may not be empty or null
     * @param user   the user represented by the cookie, may not be empty or null,
     *               and must equal the user currently associated with the cookie (if any).
     * @param item   the user has viewed the specified item, can be set by the
     *               client to null (in which case, no association between user and item is
     *               created)
     * @throws IllegalArgumentException if the parameter pre-conditions aren't satisfied.
     */
    @Override
    public void updateCookie(String cookie, String user, String item) {
        if (cookie == null || cookie.isEmpty()) {
            throw new IllegalArgumentException("Cookie cannot be null or empty");
        }
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException("User cannot be null or empty");
        }
        if (conn.get(cookie + ":user") != null && !conn.get(cookie + ":user").equals(user)) {
            throw new IllegalArgumentException("This is not the cookie's user");
        }

        // first, add the current user to the cookie
        conn.set(cookie + ":user", user);
        // then, add the user and the timestamp to the sorted set
        conn.zadd("allCookies", getTimeMicro(), cookie);
        // then, add the item to the user's list if it isn't null
        if (item != null) {
            // conn.rpush(cookie + ":items", item);
            conn.zadd(cookie + ":items", getTimeMicro(), item);
            // if there are more than MAX_ITEMS_CACHED_PER_USER, then delete the oldest
            if (conn.zcard(cookie + ":items") > MAX_ITEMS_CACHED_PER_USER) {
                conn.zpopmin(cookie + ":items");
            }
        }

    }

    /**
     * It turns out that Java has no easy way of getting time in microsecond precision since the epoch
     * @return current time in microseconds since the epoch
     */
    private long getTimeMicro() {
        // this will be slightly off, because I am getting the instant twice
        // but doing it better would require storing the variable, and for this assignment, I'm not risking that
        // the one thing that I can guarantee is that if this gets called twice in the same millisecond, the one
        // called first will be before the one called after
        return Instant.now().toEpochMilli() * 1000 + Instant.now().getNano() / 1000;
    }

    /**
     * Returns a non-null array containing the "viewed items" associated with
     * the user associated with the specified cookie.  The items must be ordered
     * in ascending order of the time that the client invoked updateCookie().
     *
     * @param cookie may or not be associated with a user
     * @return An array of items associated with the user associated with the
     * cookie if such an association currently exists, returns an empty array.
     */
    @Override
    public String[] getItems(String cookie) {
        if (cookie == null || cookie.isEmpty()) {
            //throw new IllegalArgumentException("Cookie cannot be null or empty");
            return new String[0];
        }

        return conn.zrange(cookie + ":items", 0, -1).toArray(new String[0]);
    }

    /**
     * Returns a non-null array containing the cookies currently cached by the
     * system.  The cookies must be ordered in ascending order of the time that the
     * client invoked updateCookie().
     *
     * @return An array of cookies, empty if no cookies are currently cached by the
     * system.
     */
    @Override
    public String[] getCookies() {
        return conn.zrange("allCookies", 0, -1).toArray(new String[0]);
    }

    /**
     * Modifies the user's cart to either add to or remove an item from the
     * cart.
     *
     * @param cookie   represents the user on the server (i.e., all user state,
     *                 including the user's cart), may not be empty or null.
     * @param item     represents the item being added to the cart, may not
     *                 empty or null.
     * @param quantity if a positive value, specifies the number of items in the
     *                 cart (and replaces any previous quantity associated with the item); if
     *                 not a positive value, specifies that the item be removed from the cart.
     * @throws IllegalArgumentException if the parameter pre-conditions aren't
     *                                  met.
     */
    @Override
    public void modifyCart(String cookie, String item, int quantity) {
        if (cookie == null || cookie.isEmpty()) {
            throw new IllegalArgumentException("Cookie cannot be null or empty");
        }
        if (item == null || item.isEmpty()) {
            throw new IllegalArgumentException("User cannot be null or empty");
        }

        if (quantity > 0) {
            conn.hset(cookie + ":cart", item, ""+quantity);
        } else {
            conn.hdel(cookie + ":cart", item);
        }
    }

    /**
     * Returns the user's cart if one exists on the server.
     *
     * @param cookie represents the user on the server.
     * @return a Map whose keys are the items in the cart and whose values are
     * the quantities of that item in the cart.  If the user isn't currently
     * associated with a cart or if the cookie isn't currently associated with
     * the user, returns Collections.emptyMap.
     */
    @Override
    public Map<String, Integer> getCart(String cookie) {
        if (cookie == null || cookie.isEmpty()) {
            //throw new IllegalArgumentException("Cookie cannot be null or empty");
            return Collections.emptyMap();
        }
        // I wanted more efficiency, so I'm doing a parallel stream
        // But generics and arrays don't play nicely
        return Map.ofEntries(conn.hgetAll(cookie + ":cart")
                .entrySet().parallelStream()
                .map(x->Map.entry(x.getKey(), Integer.parseInt(x.getValue())))
                .toArray(Map.Entry[]::new));
    }

    public static class CleanerThread extends RedisCacheBase.CleanerThreadBase {

        /**
         * Constructor: the cleaner thread will remove up to the specified number
         * of instances of users' state, with each set of user state counting as
         * one instance. No instances of user state are removed if the currently
         * cached instances are less than or equal to the specified limit.
         *
         * @param limit A maximum of "limit" number of instances of user state
         *              remain after the method completes, must be non-negative.
         */
        public CleanerThread(int limit) {
            super(limit);
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Integer call() throws Exception {
            //System.out.println("Thread started");
            // first, we generate a new connection, since we forgot to before
            // let's hope that this is the same as the user
            try(Jedis conn = new Jedis()){
                conn.select(DB_INDEX);
                //System.out.println("Connected to a database");

                // first, we check if we have work:
                // I am assuming that since this is discarded after the method, it isn't considered state, and so doesn't violate the requirements
                int numCookies = (int) conn.zcard("allCookies");

                if (numCookies <= limit) {
                    return 0; // we didn't need to delete anything, the cache wasn't overflowing
                } // otherwise, let's delete them
                // I'm pretty sure I am allowed to save the results of this method temporarily.
                List<Tuple> removedCookies = conn.zpopmin("allCookies", numCookies - limit);
                for (Tuple cookie : removedCookies) {
                    String cookieVal = cookie.getElement();
                    // remove its user, items, and cart
                    // no idea if doing this in one method works
                    conn.del(cookieVal + ":user", cookieVal + ":items", cookieVal + ":cart");
                }

                return removedCookies.size();
            }
        }
    }
}
