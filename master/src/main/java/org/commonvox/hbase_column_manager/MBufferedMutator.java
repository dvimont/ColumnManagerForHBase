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
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;

/**
 *
 * @author Daniel Vimont
 */
class MBufferedMutator implements BufferedMutator {

  private final BufferedMutator wrappedBufferedMutator;
  private final Repository repository;
  private final MTableDescriptor mTableDescriptor;
  private final boolean includedInRepositoryProcessing;

  MBufferedMutator(BufferedMutator userBufferedMutator, Repository repository)
          throws IOException {
    wrappedBufferedMutator = userBufferedMutator;
    this.repository = repository;
    if (this.repository.isActivated()) {
      mTableDescriptor = this.repository.getMTableDescriptor(wrappedBufferedMutator.getName());
      includedInRepositoryProcessing = repository.isIncludedTable(wrappedBufferedMutator.getName());
    } else {
      mTableDescriptor = null;
      includedInRepositoryProcessing = false;
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
  public void mutate(Mutation mutation) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, mutation);
    }
    // Alias processing
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, mutation);
      Mutation convertedMutation = (Mutation)repository.convertQualifiersToAliases(
                  mTableDescriptor, mutation, familyQualifierToAliasMap, 0);
      wrappedBufferedMutator.mutate(convertedMutation);
    } else {
    // Standard HBase processing
      wrappedBufferedMutator.mutate(mutation);
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, mutation);
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutationList) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, mutationList);
    }
    // Alias processing
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, mutationList, 0);
      List<Mutation> convertedMutations = new LinkedList<>();
      for (Mutation originalMutation : mutationList) {
        if (Mutation.class.isAssignableFrom(originalMutation.getClass())) {
          convertedMutations.add((Mutation)repository.convertQualifiersToAliases(
                  mTableDescriptor, originalMutation, familyQualifierToAliasMap, 0));
        }
      }
      wrappedBufferedMutator.mutate(convertedMutations);
    } else {
    // Standard HBase processing
      wrappedBufferedMutator.mutate(mutationList);
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, mutationList);
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
  public boolean equals(Object otherObject) {
    return wrappedBufferedMutator.equals(otherObject);
  }

  @Override
  public int hashCode() {
    return wrappedBufferedMutator.hashCode();
  }
}
