package io.goshawkdb.client;

public class TransactionRestartRequiredException extends RuntimeException {

    public static final TransactionRestartRequiredException e = new TransactionRestartRequiredException();

}
