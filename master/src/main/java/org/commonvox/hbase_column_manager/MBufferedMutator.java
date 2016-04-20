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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

/**
 *
 * @author Daniel Vimont
 */
class MBufferedMutator implements BufferedMutator {

    private final BufferedMutator wrappedBufferedMutator;
    private final Repository repository;
    private final MTableDescriptor mTableDescriptor;

    MBufferedMutator (BufferedMutator userBufferedMutator, Repository repository)
            throws IOException {
        wrappedBufferedMutator = userBufferedMutator;
        this.repository = repository;
        if (this.repository.isActivated()) {
            mTableDescriptor = this.repository.getMTableDescriptor(wrappedBufferedMutator.getName());
        } else {
            mTableDescriptor = null;
        }
    }

    @Override
    public TableName getName() {
        return wrappedBufferedMutator.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return wrappedBufferedMutator.getConfiguration();
    }

    @Override
    public void mutate(Mutation mtn) throws IOException {
        // ColumnManager validation
        if (repository.isActivated()
                && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
            repository.validateColumns(mTableDescriptor, mtn);
        }
        // Standard HBase processing
        wrappedBufferedMutator.mutate(mtn);
        // ColumnManager auditing
        if (repository.isActivated()) {
            repository.putColumnAuditors(mTableDescriptor, mtn);
        }
    }

    @Override
    public void mutate(List<? extends Mutation> list) throws IOException {
        // ColumnManager validation
        if (repository.isActivated()
                && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
            repository.validateColumns(mTableDescriptor, list);
        }
        // Standard HBase processing
        wrappedBufferedMutator.mutate(list);
        // ColumnManager auditing
        if (repository.isActivated()) {
            repository.putColumnAuditors(mTableDescriptor, list);
        }
    }

    @Override
    public void close() throws IOException {
        wrappedBufferedMutator.close();
    }

    @Override
    public void flush() throws IOException {
        wrappedBufferedMutator.flush();
    }

    @Override
    public long getWriteBufferSize() {
        return wrappedBufferedMutator.getWriteBufferSize();
    }

    @Override
    public boolean equals (Object otherObject) {
        return wrappedBufferedMutator.equals(otherObject);
    }

    @Override
    public int hashCode() {
        return wrappedBufferedMutator.hashCode();
    }
}
