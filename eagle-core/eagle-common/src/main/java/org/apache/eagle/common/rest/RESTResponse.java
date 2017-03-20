/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.common.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.eagle.common.function.ThrowableConsumer;
import org.apache.eagle.common.function.ThrowableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class RESTResponse<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RESTResponse.class);

    @JsonProperty
    private boolean success = false;
    @JsonProperty
    private String message;
    @JsonProperty
    private String exception;
    @JsonProperty
    private T data;

    public RESTResponse() {
    }

    public RESTResponse(Throwable throwable) {
        if (throwable.getMessage() == null || throwable.getMessage().isEmpty()) {
            this.setMessage(throwable.getMessage());
        } else {
            this.setMessage(ExceptionUtils.getMessage(throwable));
        }
        this.setException(ExceptionUtils.getStackTrace(throwable));
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public static <E> RestResponseBuilder<E> builder() {
        return new RestResponseBuilder<>();
    }

    public static <E> RestResponseBuilder<E> of(E data) {
        return RESTResponse.<E>builder().data(data);
    }

    public static <E> RestResponseBuilder<E> of(Consumer<RestResponseBuilder<E>> func) {
        return RESTResponse.<E>builder().of(func);
    }

    public static <E> RestResponseBuilder<E> of(Supplier<E> func) {
        return RESTResponse.<E>builder().of(func);
    }

    public static <E> RestResponseBuilder<E> async(ThrowableSupplier<E, Exception> func) {
        return RESTResponse.<E>builder().async(func);
    }

    public static <E> RestResponseBuilder<E> async(ThrowableConsumer<RestResponseBuilder<E>, Exception> func) {
        return RESTResponse.<E>builder().async(func);
    }

    public String getException() {
        return exception;
    }

    public void setThrowable(Throwable exception) {
        this.setException(ExceptionUtils.getStackTrace(exception));
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public static class RestResponseBuilder<E> {
        private RESTResponse current = new RESTResponse();
        private Response.Status status = Response.Status.OK;
        private CompletableFuture future = null;

        public RestResponseBuilder<E> success(boolean success) {
            this.current.setSuccess(success);
            return this;
        }

        public RestResponseBuilder<E> status(Response.Status status) {
            this.status = status;
            return this;
        }

        public RestResponseBuilder<E> status(boolean success, Response.Status status) {
            this.success(success);
            this.status(status);
            return this;
        }

        public RestResponseBuilder<E> message(String message) {
            this.current.setMessage(message);
            return this;
        }

        public RestResponseBuilder<E> data(E data) {
            this.current.setData(data);
            return this;
        }

        public RestResponseBuilder<E> exception(Throwable exception) {
            this.current.setThrowable(exception);
            if (this.current.getMessage() == null) {
                if (exception.getMessage() == null || exception.getMessage().isEmpty()) {
                    this.current.setMessage(ExceptionUtils.getMessage(exception));
                } else {
                    this.current.setMessage(exception.getMessage());
                }
            }
            return this;
        }

        public RestResponseBuilder<E> of(Consumer<RestResponseBuilder<E>> func) {
            try {
                this.success(true).status(Response.Status.OK);
                func.accept(this);
            } catch (Exception ex) {
                LOGGER.error("Exception: " + ex.getMessage(), ex);
                this.success(false).data(null).status(Response.Status.BAD_REQUEST).exception(ex);
                raiseWebAppException(ex);
            }
            return this;
        }

        public RestResponseBuilder<E> of(Supplier<E> func) {
            try {
                this.success(true).status(Response.Status.OK).data(func.get());
            } catch (Throwable ex) {
                LOGGER.error("Exception: " + ex.getMessage(), ex);
                this.success(false).status(Response.Status.BAD_REQUEST).exception(ex);
                raiseWebAppException(ex);
            }
            return this;
        }

        public RestResponseBuilder<E> async(ThrowableSupplier<E, Exception> func) {
            CompletableFuture future = CompletableFuture.runAsync(() -> {
                try {
                    this.status(Response.Status.OK).success(true).data(func.get());
                } catch (Throwable e) {
                    LOGGER.error("Exception: " + e.getMessage(), e);
                    this.success(false).status(Response.Status.BAD_REQUEST).exception(e);
                    raiseWebAppException(e);
                }
            });
            runAsync(future);
            return this;
        }

        public RestResponseBuilder<E> async(ThrowableConsumer<RestResponseBuilder<E>, Exception> func) {
            CompletableFuture future = CompletableFuture.runAsync(() -> {
                try {
                    func.accept(this);
                    this.success(true);
                } catch (Throwable ex) {
                    LOGGER.error("Exception: " + ex.getMessage(), ex);
                    this.success(false).status(Response.Status.BAD_REQUEST).exception(ex);
                    raiseWebAppException(ex);
                }
            });
            runAsync(future);
            return this;
        }

        private void runAsync(Future future) {
            try {
                future.get();
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException: " + ex.getMessage(), ex);
                Thread.currentThread().interrupt();
                future.cancel(true);
                this.success(false).status(Response.Status.BAD_REQUEST).exception(ex.getCause());
                raiseWebAppException(ex);
            } catch (ExecutionException ex) {
                LOGGER.error("ExecutionException: " + ex.getMessage(), ex);
                this.success(false).status(Response.Status.BAD_REQUEST).exception(ex.getCause());
                raiseWebAppException(ex);
            }
        }

        private void raiseWebAppException(Throwable ex) {
            throw new WebApplicationException(ex, Response.status(this.status).entity(this.current).build());
        }


        public RestResponseBuilder<E> then(ThrowableConsumer<RestResponseBuilder<E>, Exception> func) {
            try {
                func.accept(this);
            } catch (Throwable ex) {
                LOGGER.error("Exception: " + ex.getMessage(), ex);
                this.success(false).status(Response.Status.BAD_REQUEST).exception(ex);
                raiseWebAppException(ex);
            }
            return this;
        }

        public RESTResponse<E> get() {
            return current;
        }

        public Response build() {
            return Response.status(status).entity(current).type(MediaType.APPLICATION_JSON).build();
        }
    }
}