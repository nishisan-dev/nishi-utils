/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.queue.exceptions;

import dev.nishisan.requests.common.exception.BasicIOException;
import dev.nishisan.requests.common.request.IRequest;

import java.util.Map;

public class NQueueIOException extends BasicIOException {
    public NQueueIOException() {
    }

    public NQueueIOException(IRequest<?> request) {
        super(request);
    }

    public NQueueIOException(String message) {
        super(message);
    }

    public NQueueIOException(String message, Throwable th) {
        super(message, th);
    }

    public NQueueIOException(String message, IRequest<?> request) {
        super(message, request);
    }

    public NQueueIOException(IRequest<?> request, Integer statusCode, Map<String, Object> details) {
        super(request, statusCode, details);
    }

    public NQueueIOException(String message, IRequest<?> request, Integer statusCode, Map<String, Object> details) {
        super(message, request, statusCode, details);
    }

    public NQueueIOException(String message, Throwable cause, IRequest<?> request, Integer statusCode, Map<String, Object> details) {
        super(message, cause, request, statusCode, details);
    }

    public NQueueIOException(Throwable cause, IRequest<?> request, Integer statusCode, Map<String, Object> details) {
        super(cause, request, statusCode, details);
    }

    public NQueueIOException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, IRequest<?> request, Integer statusCode, Map<String, Object> details) {
        super(message, cause, enableSuppression, writableStackTrace, request, statusCode, details);
    }

    @Override
    public  NQueueIOException details(String s, Object o) {
        this.addDetail(s,o);
        return this;
    }
}
