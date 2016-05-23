package io.goshawkdb.client;

public class TransactionRestartRequiredException extends RuntimeException {

    static final TransactionRestartRequiredException e = new TransactionRestartRequiredException();

}
