/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.manifest;

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageLegacyV2Serializer;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link VersionedSerializer} for {@link ManifestCommittable}. */
public class ManifestCommittableSerializer implements VersionedSerializer<ManifestCommittable> {

    private static final int CURRENT_VERSION = 6;
    private static final int WRITE_COLS_DICTIONARY_VERSION = 6;
    private static final int NO_WRITE_COLS = -1;

    private final CommitMessageSerializer commitMessageSerializer;

    private CommitMessageLegacyV2Serializer legacyV2CommitMessageSerializer;

    public ManifestCommittableSerializer() {
        this.commitMessageSerializer = new CommitMessageSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ManifestCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeLong(obj.identifier());
        Long watermark = obj.watermark();
        if (watermark == null) {
            view.writeBoolean(true);
        } else {
            view.writeBoolean(false);
            view.writeLong(watermark);
        }
        serializeProperties(view, obj.properties());
        WriteColsDictionary dictionary = collectWriteColsDictionary(obj);
        serializeWriteColsDictionary(view, dictionary);
        view.writeInt(commitMessageSerializer.getVersion());
        IntReferences references = new IntReferences(countFiles(obj));
        commitMessageSerializer.serializeList(
                stripCommitMessages(obj.fileCommittables(), dictionary, references), view);
        references.serialize(view);
        return out.toByteArray();
    }

    private void serializeProperties(
            DataOutputViewStreamWrapper view, Map<String, String> properties) throws IOException {
        view.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            view.writeUTF(entry.getKey());
            view.writeUTF(entry.getValue());
        }
    }

    @Override
    public ManifestCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting ManifestCommittableSerializer version to be smaller or equal than "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        DataInputDeserializer view = new DataInputDeserializer(serialized);
        long identifier = view.readLong();
        Long watermark = view.readBoolean() ? null : view.readLong();
        if (version <= 4) {
            skipLegacyLogOffsets(view);
        }
        Map<String, String> properties =
                version >= 4 ? deserializeProperties(view) : new HashMap<>();
        List<WriteColsKey> dictionary =
                version >= WRITE_COLS_DICTIONARY_VERSION
                        ? deserializeWriteColsDictionary(view)
                        : null;
        int fileCommittableSerializerVersion = view.readInt();
        List<CommitMessage> fileCommittables;
        try {
            fileCommittables =
                    commitMessageSerializer.deserializeList(fileCommittableSerializerVersion, view);
        } catch (Exception e) {
            if (fileCommittableSerializerVersion != 2) {
                throw e;
            }

            // rebuild view
            view = new DataInputDeserializer(serialized);
            view.readLong();
            if (!view.readBoolean()) {
                view.readLong();
            }
            skipLegacyLogOffsets(view);
            view.readInt();

            if (legacyV2CommitMessageSerializer == null) {
                legacyV2CommitMessageSerializer = new CommitMessageLegacyV2Serializer();
            }
            fileCommittables = legacyV2CommitMessageSerializer.deserializeList(view);
        }

        if (dictionary != null) {
            IntReferences references = IntReferences.deserialize(view);
            fileCommittables = restoreCommitMessages(fileCommittables, dictionary, references);
        }

        return new ManifestCommittable(identifier, watermark, fileCommittables, properties);
    }

    private WriteColsDictionary collectWriteColsDictionary(ManifestCommittable committable) {
        WriteColsDictionary dictionary = new WriteColsDictionary();
        for (CommitMessage commitMessage : committable.fileCommittables()) {
            CommitMessageImpl message = (CommitMessageImpl) commitMessage;
            collectWriteCols(message.newFilesIncrement().newFiles(), dictionary);
            collectWriteCols(message.newFilesIncrement().deletedFiles(), dictionary);
            collectWriteCols(message.newFilesIncrement().changelogFiles(), dictionary);
            collectWriteCols(message.compactIncrement().compactBefore(), dictionary);
            collectWriteCols(message.compactIncrement().compactAfter(), dictionary);
            collectWriteCols(message.compactIncrement().changelogFiles(), dictionary);
        }
        return dictionary;
    }

    private void collectWriteCols(List<DataFileMeta> files, WriteColsDictionary dictionary) {
        for (DataFileMeta file : files) {
            dictionary.register(file);
        }
    }

    private void serializeWriteColsDictionary(
            DataOutputViewStreamWrapper view, WriteColsDictionary dictionary) throws IOException {
        view.writeInt(dictionary.entries.size());
        for (WriteColsKey key : dictionary.entries.keySet()) {
            view.writeLong(key.schemaId);
            view.writeInt(key.writeCols.size());
            for (String writeCol : key.writeCols) {
                view.writeUTF(writeCol);
            }
        }
    }

    private List<WriteColsKey> deserializeWriteColsDictionary(DataInputDeserializer view)
            throws IOException {
        int size = view.readInt();
        List<WriteColsKey> dictionary = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long schemaId = view.readLong();
            int columnCount = view.readInt();
            List<String> writeCols = new ArrayList<>(columnCount);
            for (int j = 0; j < columnCount; j++) {
                writeCols.add(view.readUTF());
            }
            dictionary.add(new WriteColsKey(schemaId, writeCols, false));
        }
        return dictionary;
    }

    private List<CommitMessage> stripCommitMessages(
            List<CommitMessage> commitMessages,
            WriteColsDictionary dictionary,
            IntReferences references) {
        List<CommitMessage> stripped = new ArrayList<>(commitMessages.size());
        for (CommitMessage commitMessage : commitMessages) {
            stripped.add(stripWriteCols((CommitMessageImpl) commitMessage, dictionary, references));
        }
        return stripped;
    }

    private CommitMessage stripWriteCols(
            CommitMessageImpl message, WriteColsDictionary dictionary, IntReferences references) {
        DataIncrement data = message.newFilesIncrement();
        CompactIncrement compact = message.compactIncrement();
        return new CommitMessageImpl(
                message.partition(),
                message.bucket(),
                message.totalBuckets(),
                new DataIncrement(
                        stripDataFiles(data.newFiles(), dictionary, references),
                        stripDataFiles(data.deletedFiles(), dictionary, references),
                        stripDataFiles(data.changelogFiles(), dictionary, references),
                        data.newIndexFiles(),
                        data.deletedIndexFiles()),
                new CompactIncrement(
                        stripDataFiles(compact.compactBefore(), dictionary, references),
                        stripDataFiles(compact.compactAfter(), dictionary, references),
                        stripDataFiles(compact.changelogFiles(), dictionary, references),
                        compact.newIndexFiles(),
                        compact.deletedIndexFiles()));
    }

    private List<DataFileMeta> stripDataFiles(
            List<DataFileMeta> files, WriteColsDictionary dictionary, IntReferences references) {
        List<DataFileMeta> stripped = new ArrayList<>(files.size());
        for (DataFileMeta file : files) {
            List<String> writeCols = file.writeCols();
            references.add(writeCols == null ? NO_WRITE_COLS : dictionary.reference(file));
            stripped.add(writeCols == null ? file : copyWithWriteCols(file, null));
        }
        return stripped;
    }

    private List<CommitMessage> restoreCommitMessages(
            List<CommitMessage> commitMessages,
            List<WriteColsKey> dictionary,
            IntReferences references)
            throws IOException {
        int expectedFiles = countFiles(commitMessages);
        if (references.size() != expectedFiles) {
            throw new IOException(
                    "Corrupt writeCols references: expected "
                            + expectedFiles
                            + " but found "
                            + references.size());
        }

        List<CommitMessage> restored = new ArrayList<>(commitMessages.size());
        for (CommitMessage commitMessage : commitMessages) {
            restored.add(
                    restoreWriteCols((CommitMessageImpl) commitMessage, dictionary, references));
        }
        references.checkComplete();
        return restored;
    }

    private CommitMessage restoreWriteCols(
            CommitMessageImpl message, List<WriteColsKey> dictionary, IntReferences references)
            throws IOException {
        DataIncrement data = message.newFilesIncrement();
        CompactIncrement compact = message.compactIncrement();
        return new CommitMessageImpl(
                message.partition(),
                message.bucket(),
                message.totalBuckets(),
                new DataIncrement(
                        restoreDataFiles(data.newFiles(), dictionary, references),
                        restoreDataFiles(data.deletedFiles(), dictionary, references),
                        restoreDataFiles(data.changelogFiles(), dictionary, references),
                        data.newIndexFiles(),
                        data.deletedIndexFiles()),
                new CompactIncrement(
                        restoreDataFiles(compact.compactBefore(), dictionary, references),
                        restoreDataFiles(compact.compactAfter(), dictionary, references),
                        restoreDataFiles(compact.changelogFiles(), dictionary, references),
                        compact.newIndexFiles(),
                        compact.deletedIndexFiles()));
    }

    private List<DataFileMeta> restoreDataFiles(
            List<DataFileMeta> files, List<WriteColsKey> dictionary, IntReferences references)
            throws IOException {
        List<DataFileMeta> restored = new ArrayList<>(files.size());
        for (DataFileMeta file : files) {
            int reference = references.next();
            if (reference == NO_WRITE_COLS) {
                restored.add(file);
                continue;
            }
            if (reference < 0 || reference >= dictionary.size()) {
                throw new IOException("Corrupt writeCols dictionary reference: " + reference);
            }
            WriteColsKey key = dictionary.get(reference);
            if (key.schemaId != file.schemaId()) {
                throw new IOException(
                        "Corrupt writeCols dictionary schema id: expected "
                                + file.schemaId()
                                + " but found "
                                + key.schemaId);
            }
            restored.add(copyWithWriteCols(file, key.writeCols));
        }
        return restored;
    }

    private DataFileMeta copyWithWriteCols(DataFileMeta file, List<String> writeCols) {
        return DataFileMeta.create(
                file.fileName(),
                file.fileSize(),
                file.rowCount(),
                file.minKey(),
                file.maxKey(),
                file.keyStats(),
                file.valueStats(),
                file.minSequenceNumber(),
                file.maxSequenceNumber(),
                file.schemaId(),
                file.level(),
                file.extraFiles(),
                file.creationTime(),
                file.deleteRowCount().orElse(null),
                file.embeddedIndex(),
                file.fileSource().orElse(null),
                file.valueStatsCols(),
                file.externalPath().orElse(null),
                file.firstRowId(),
                writeCols,
                file.columnMaxSequenceNumbers());
    }

    private int countFiles(ManifestCommittable committable) {
        return countFiles(committable.fileCommittables());
    }

    private int countFiles(List<CommitMessage> commitMessages) {
        int count = 0;
        for (CommitMessage commitMessage : commitMessages) {
            CommitMessageImpl message = (CommitMessageImpl) commitMessage;
            DataIncrement data = message.newFilesIncrement();
            CompactIncrement compact = message.compactIncrement();
            count +=
                    data.newFiles().size()
                            + data.deletedFiles().size()
                            + data.changelogFiles().size()
                            + compact.compactBefore().size()
                            + compact.compactAfter().size()
                            + compact.changelogFiles().size();
        }
        return count;
    }

    private void skipLegacyLogOffsets(DataInputDeserializer view) throws IOException {
        int size = view.readInt();
        for (int i = 0; i < size; i++) {
            view.readInt();
            view.readLong();
        }
    }

    private Map<String, String> deserializeProperties(DataInputDeserializer view)
            throws IOException {
        int size = view.readInt();
        Map<String, String> properties = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            properties.put(view.readUTF(), view.readUTF());
        }
        return properties;
    }

    private static class WriteColsKey {

        private final long schemaId;
        private final List<String> writeCols;

        private WriteColsKey(long schemaId, List<String> writeCols, boolean copy) {
            this.schemaId = schemaId;
            this.writeCols = copy ? new ArrayList<>(writeCols) : writeCols;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WriteColsKey)) {
                return false;
            }
            WriteColsKey that = (WriteColsKey) o;
            return schemaId == that.schemaId && writeCols.equals(that.writeCols);
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(schemaId) + writeCols.hashCode();
        }
    }

    private static class WriteColsDictionary {

        private final Map<WriteColsKey, Integer> entries = new LinkedHashMap<>();
        private final Map<DataFileMeta, Integer> fileReferences = new IdentityHashMap<>();

        private void register(DataFileMeta file) {
            List<String> writeCols = file.writeCols();
            if (writeCols == null) {
                return;
            }

            WriteColsKey lookup = new WriteColsKey(file.schemaId(), writeCols, false);
            Integer reference = entries.get(lookup);
            if (reference == null) {
                reference = entries.size();
                entries.put(new WriteColsKey(file.schemaId(), writeCols, true), reference);
            }
            fileReferences.put(file, reference);
        }

        private int reference(DataFileMeta file) {
            Integer reference = fileReferences.get(file);
            if (reference == null) {
                throw new IllegalStateException(
                        "Missing writeCols dictionary reference for " + file.fileName());
            }
            return reference;
        }
    }

    private static class IntReferences {

        private final int[] values;
        private int position;

        private IntReferences(int size) {
            this.values = new int[size];
        }

        private void add(int value) {
            values[position++] = value;
        }

        private int next() throws IOException {
            if (position >= values.length) {
                throw new IOException("Corrupt writeCols references: no reference left");
            }
            return values[position++];
        }

        private int size() {
            return values.length;
        }

        private void checkComplete() {
            if (position != values.length) {
                throw new IllegalStateException(
                        "Incomplete writeCols references: expected "
                                + values.length
                                + " but processed "
                                + position);
            }
        }

        private void serialize(DataOutputViewStreamWrapper view) throws IOException {
            checkComplete();
            view.writeInt(values.length);
            for (int value : values) {
                view.writeInt(value);
            }
        }

        private static IntReferences deserialize(DataInputDeserializer view) throws IOException {
            int size = view.readInt();
            if (size < 0) {
                throw new IOException("Corrupt writeCols references size: " + size);
            }
            IntReferences references = new IntReferences(size);
            for (int i = 0; i < size; i++) {
                references.values[i] = view.readInt();
            }
            return references;
        }
    }
}
