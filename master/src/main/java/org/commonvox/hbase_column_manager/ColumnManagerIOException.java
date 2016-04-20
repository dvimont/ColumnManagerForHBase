/*
 * Copyright (C) 2016 Daniel Vimont
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.commonvox.hbase_column_manager;

import java.io.IOException;

/**
 * All {@code IOException}s in the ColumnManagerAPI package are subclasses of the abstract
 * {@code ColumnManagerIOException}.
 * @author Daniel Vimont
 */
public abstract class ColumnManagerIOException extends IOException {

    /**
     * Constructs an instance of {@code ColumnManagerIOException} with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    ColumnManagerIOException(String msg) {
        super(msg);
    }
}
