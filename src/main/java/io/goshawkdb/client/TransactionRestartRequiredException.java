package io.goshawkdb.client;

class TransactionRestartRequiredException extends RuntimeException {

    static final TransactionRestartRequiredException e = new TransactionRestartRequiredException();

}
