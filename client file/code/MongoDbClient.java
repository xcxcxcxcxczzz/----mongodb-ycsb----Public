/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import org.bson.Document;
import org.bson.types.Binary;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class MongoDbClient extends DB {

  /** The bulk inserts pending for the thread. */
  private List<Document> bulkInserts = new ArrayList<Document>();
  private List<Document> prevBulkInserts = new ArrayList<Document>();
  private  Cipher encCipher;
  private  Cipher decCipher;
  private Mac macInstance;
  private Random RAND = new Random();
  private String linkLabel = "link_data",
      keyLabel = "_id",
      authLabel = "auth";
  private int linkCount;
  private  Set<String> verifyKeysSet = new HashSet<>();
  private  Set<String> modifiedKeys = new HashSet<>();
  private  Set<String> missingKeys = new HashSet<>();

  private static String databaseName;
  private static ReadPreference readPreference;
  private static WriteConcern writeConcern;
  private static MongoClient mongoClient;
  private static MongoDatabase database;
  private static final Charset CHARSET = Charset.forName("UTF-8");

  static Class<? extends List> linkDataClazz = new ArrayList<String>().getClass();


  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      Properties props = getProperties();

      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
            + "or 'mongodb+srv://<host>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1000"));
      linkCount = Integer.parseInt(props.getProperty("linkcount", "4"));

      String authKey = props.getProperty("authkey", "0123456789123456");
      String encKey = props.getProperty("enckey", "0123456789123456");

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);

        macInstance = Mac.getInstance("HmacSHA256");
        SecretKeySpec km = new SecretKeySpec(authKey.getBytes(), "HmacSHA256");
        macInstance.init(km);
        SecretKey ke = new SecretKeySpec(encKey.getBytes(), "AES");
        decCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        decCipher.init(Cipher.DECRYPT_MODE, ke);
        encCipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        encCipher.init(Cipher.ENCRYPT_MODE, ke);
      }
      catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {

    try {
      Document toInsert = new Document("_id", key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.put(entry.getKey(), entry.getValue().toArray());
      }

//      if (batchSize == 1) {
//        if (useUpsert) {
//          // this is effectively an insert, but using an upsert instead due
//          // to current inability of the framework to clean up after itself
//          // between test runs.
//          collection.replaceOne(new Document("_id", toInsert.get("_id")),
//              toInsert, UPDATE_WITH_UPSERT);
//        } else {
//          collection.insertOne(toInsert);
//        }
//      }

      bulkInserts.add(toInsert);
      if (bulkInserts.size() == batchSize) {
//          if (useUpsert) {
//            List<UpdateOneModel<Document>> updates =
//                new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
//            for (Document doc : bulkInserts) {
//              updates.add(new UpdateOneModel<Document>(
//                  new Document("_id", doc.get("_id")),
//                  doc, UPDATE_WITH_UPSERT));
//            }
//            collection.bulkWrite(updates);
//          } else {
        ProcessWrite();
        MongoCollection<Document> collection = database.getCollection(table);
        collection.insertMany(bulkInserts, INSERT_UNORDERED);
//          }
        prevBulkInserts = bulkInserts;
        bulkInserts = new ArrayList<>();
      } else {
        return Status.BATCHED_OK;
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      System.out.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);

      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();
      ProcessRead(queryResult);
      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        ProcessRead(obj);
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   *
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }
  private void ProcessWrite() {
    // create key-value map
    Map<String, Document> tuples = new HashMap<>();

    for (Document doc : bulkInserts) {
      tuples.put(doc.getString(keyLabel), doc);
    }
    List<String> id_set = new ArrayList<>(tuples.keySet());

    for (Document doc : prevBulkInserts) {
      LinkDoc(doc, tuples, id_set);
    }
    for (Document doc : bulkInserts) {
      LinkDoc(doc, tuples, id_set);
    }

    for (Document doc : bulkInserts) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        if (doc.containsKey(linkLabel)) {
          String link_data = objectMapper.writeValueAsString(doc.get(linkLabel, linkDataClazz));
            doc.replace(linkLabel,
                Base64.getEncoder().encodeToString(encCipher.doFinal(link_data.getBytes(CHARSET))));
        }
        doc.append(authLabel,
            new String( macInstance.doFinal(doc.toString().getBytes())));
      } catch (Exception e) {
        System.out.println("encoding error" + e.toString());
      }
    }
  }
  private List<String> GenLink(String key, List<String> linking_keys) {
    Set<String> result = new HashSet<>();
    while (result.size() < linkCount/2) {
      String nextKey = linking_keys.get(RAND.nextInt(linking_keys.size()));
      if (!nextKey.equals(key)) {
        result.add(nextKey);
      }
    }
    return new ArrayList<>(result);
  }
  private void LinkDoc(Document doc, Map<String, Document> tuples, List<String> linkfrom_set) {
    String doc_key = doc.getString(keyLabel);
    for (String from : GenLink(doc_key, linkfrom_set)) {
      Document linking_doc = tuples.get(from);
      if (linking_doc.containsKey(linkLabel)) {
        List<String> link_data = linking_doc.get(linkLabel, linkDataClazz);
        link_data.add(doc_key);
        linking_doc.replace(linkLabel, link_data);
      }
      else {
        List<String> link_data = new ArrayList<>();
        link_data.add(doc_key);
        //if (linking_doc != null )
          linking_doc.append(linkLabel, link_data);
      }
    }
  }
  private void ProcessRead(Document doc){
    String mac_data = doc.getString(authLabel);
    doc.remove(authLabel);
    String s = new String( macInstance.doFinal(doc.toString().getBytes()));
    if (!s.equals(mac_data))
      modifiedKeys.add(doc.getString(keyLabel));
    if (!doc.containsKey(linkLabel)) return;
    String link_data = doc.getString(linkLabel);
    try {
      String dec_data = new String(decCipher.doFinal(Base64.getDecoder().decode(link_data)), CHARSET);
      List<String> linked_keys = Document.parse("{\"list\":" + dec_data +"}").get("list", linkDataClazz);
      doc.remove(linkLabel);
      verifyKeysSet.addAll(linked_keys);
    } catch (Exception e) {
      System.out.println("extract link error" + e.toString());
    }
  }

}
