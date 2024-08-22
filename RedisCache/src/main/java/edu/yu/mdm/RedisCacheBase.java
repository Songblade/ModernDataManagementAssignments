package edu.yu.mdm;

/** Defines the API for the RedisCache assignment: see the requirements
 * document for more information.
 *
 * Please note the public constants defined below (and follow the implied
 * semantics).
 *
 * Students MAY NOT add to the constructor IMPLEMENTATION.  Students MAY NOT
 * add additional constructor signatures.  The same "no alteration" rules apply
 * to the inner CleanerThreadBase class.  My test code will only invoke the API
 * defined below.
 *
 * @author Avraham Leff
 */

import java.util.Map;
import java.util.concurrent.Callable;
import redis.clients.jedis.Jedis;

public abstract class RedisCacheBase {

    /** Inner class defines an API through which clients ask that "old state" be
     * removed.
     *
     * Users are removed in oldest-to-most-recent order of time that the user
     * accessed the RedisCache.  Deleting a user implies that all state
     * associated with this user is also deleted (including cookies and shopping
     * carts).
     *
     * Your implementation is allowed to ignore the race condition potentially
     * caused by concurrent method invocation that adds state to the cache (and
     * thus changes the definition of "age" for a given client) against
     * CleanerThread operation.
     *
     * "Callable.call()" returns the number of user instances deleted by the
     * CleanerThread's execution.
     */
    public abstract static class CleanerThreadBase implements Callable<Integer> {
        /** Constructor: the cleaner thread will remove up to the specified number
         * of instances of users' state, with each set of user state counting as
         * one instance.  No instances of user state are removed if the currently
         * cached instances are less than are equal to the specified limit.
         *
         * @param limit A maximum of "limit" number of instances of user state
         * remain after the method completes, must be non-negative.
         */
        public CleanerThreadBase(final int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException
                        ("limit must be non-negative: "+limit);
            }

            this.limit = limit;
        } // constructor

        // not private by design
        protected final int limit;
    }


    /** The index of the Redis database to which ALL client requests must be
     * addressed.
     */
    public static final int DB_INDEX = 15;

    /** Maximum number of items cached per user.
     */
    public static final int MAX_ITEMS_CACHED_PER_USER = 25;

    /** Constructor is supplied with a Jedis instance, created with the no-arg
     * constructor.  The constructor selects DB number DB_INDEX and then empties
     * the current contents of that database.
     */
    public RedisCacheBase(final Jedis conn) {
        this.conn = conn;
        conn.select(DB_INDEX);
        conn.flushDB();
    }

    /** Returns the user associated with the cookie if an association currently
     * exists.
     *
     * @param cookie a cookie maintained by the server that may map to a user
     * @return the user associated with the cookie or null if no such association
     * exists (possibly because it was "aged out")
     */
    abstract public String checkCookie(String cookie);

    /** Creates (or updates) an association between a token, a user, and an item.
     * With respect to "aging out" the cookie/user association, invoking this
     * method refreshes the age to the time that the method is invoked.  The
     * implementation maintains user-to-item state for at most recent
     * MAX_ITEMS_CACHED_PER_USER: older state is discarded.
     *
     * @param cookie represents the user on the server, may not be empty or null
     * @param user the user represented by the cookie, may not be empty or null,
     * and must equal the user currently associated with the cookie (if any).
     * @param item the user has viewed the specified item, can be set by the
     * client to null (in which case, no association between user and item is
     * created)
     * @throws IllegalArgumentException if the parameter pre-conditions aren't satisfied.
     */
    abstract public void updateCookie(String cookie, String user, String item);

    /** Returns a non-null array containing the "viewed items" associated with
     * the user associated with the specified cookie.  The items must be ordered
     * in ascending order of the time that the client invoked updateCookie().
     *
     * @param cookie may or not be associated with a user
     * @return An array of items associated with the user associated with the
     * cookie if such an association currently exists, returns an empty array.
     */
    abstract public String[] getItems(String cookie);

    /** Returns a non-null array containing the cookies currently cached by the
     * system.  The cookies must be ordered in ascending order of the time that the
     * client invoked updateCookie().
     *
     * @return An array of cookies, empty if no cookies are currently cached by the
     * system.
     */
    abstract public String[] getCookies();


    /** Modifies the user's cart to either add to or remove an item from the
     * cart.
     *
     * @param cookie represents the user on the server (i.e., all user state,
     * including the user's cart), may not be empty or null.
     * @param item represents the item being added to the cart, may not
     * empty or null.
     * @param quantity if a positive value, specifies the number of items in the
     *  cart (and replaces any previous quantity associated with the item); if
     *  not a positive value, specifies that the item be removed from the cart.
     * @throws IllegalArgumentException if the parameter pre-conditions aren't
     * met.
     */
    abstract public void modifyCart(String cookie, String item, int quantity);

    /** Returns the user's cart if one exists on the server.
     *
     * @param cookie represents the user on the server.
     * @return a Map whose keys are the items in the cart and whose values are
     * the quantities of that item in the cart.  If the user isn't currently
     * associated with a cart or if the cookie isn't currently associated with
     * the user, returns Collections.emptyMap.
     */
    abstract public Map<String, Integer> getCart(String cookie);


    // explicit protected access so subclass implementation can use
    protected final Jedis conn;
} // RedisCacheBase