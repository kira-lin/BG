package mongodb;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class MongoBGClient extends DB {

	MongoClient mongoClient;
	private String ipAddress;

	public static final String MONGO_DB_NAME = "BG";
	public static final String MONGO_USER_COLLECTION = "users";
	public static final String KEY_FRIEND = "f";
	public static final String KEY_PENDING = "p";
	public static final String KEY_MONGO_DB_IP = "mongoip";

	public static final int LIST_FRIENDS = 10;

	Map<Integer, Set<String>> friendList = new HashMap<>();

	public Map<Integer, Set<String>> getFriendList() {
		return friendList;
	}

	public MongoBGClient(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public MongoBGClient() {
		java.util.logging.Logger mongoLogger = java.util.logging.Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);
	}

	private final Logger log = LoggerFactory.getLogger(MongoBGClient.class);

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		Document doc = new Document();
		doc.put("_id", entityPK);
		values.forEach((k, v) -> {
			doc.put(k, v.toString());
		});
		doc.put(KEY_FRIEND, new HashSet<Integer>());
		doc.put(KEY_PENDING, new HashSet<Integer>());
		coll.insertOne(doc);
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		List<Bson> queries = new ArrayList<Bson>();
		queries.add(new BasicDBObject("$match",
				new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
		BasicDBObject obj = new BasicDBObject();
		obj.put("f", new BasicDBObject("$size", "$f"));
		obj.put("p", new BasicDBObject("$size", "$p"));
		obj.put("username", 1);
		obj.put("pw", 1);
		obj.put("fname", 1);
		obj.put("lname", 1);
		obj.put("gender", 1);
		obj.put("dob", 1);
		obj.put("jdate", 1);
		obj.put("ldate", 1);
		obj.put("address", 1);
		obj.put("email", 1);
		obj.put("tel", 1);
		BasicDBObject bobj = new BasicDBObject("$project", obj);
		queries.add(bobj);

		Document userProfile = coll.aggregate(queries).first();
		result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes()));

		userProfile.forEach((k, v) -> {
			if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
				result.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
			}
		});

		result.put("friendcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_FRIEND)).getBytes()));
		if (requesterID == profileOwnerID) {
			result.put("pendingcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_PENDING)).getBytes()));
		}
		return 0;
	}

	public Document viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		Document userProfile = coll.find(eq("_id", String.valueOf(profileOwnerID))).first();
		userProfile.forEach((k, v) -> {
			if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
				result.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
			}
		});
		return userProfile;
	}

	@Override
	public void buildIndexes(Properties props) {
		super.buildIndexes(props);
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		List<Bson> list = new ArrayList<>();
		list.add(new BasicDBObject("$match",
				new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
		BasicDBList field = new BasicDBList();
		field.add("$f");
		field.add(0);
		field.add(LIST_FRIENDS);
		BasicDBObject bobj = new BasicDBObject("$project", new BasicDBObject("f", new BasicDBObject("$slice", field)));
		list.add(bobj);
		Document userProfile = coll.aggregate(list).first();
		List<String> friends = userProfile.get(KEY_FRIEND, List.class);

		List<Bson> queries = new ArrayList<Bson>();
		queries.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", friends))));
		BasicDBObject obj = new BasicDBObject();
		obj.put("f", new BasicDBObject("$size", "$f"));
		obj.put("username", 1);
		obj.put("pw", 1);
		obj.put("fname", 1);
		obj.put("lname", 1);
		obj.put("gender", 1);
		obj.put("dob", 1);
		obj.put("jdate", 1);
		obj.put("ldate", 1);
		obj.put("address", 1);
		obj.put("email", 1);
		obj.put("tel", 1);
		bobj = new BasicDBObject("$project", obj);
		queries.add(bobj);
		queries.add(new BasicDBObject("$limit", LIST_FRIENDS));

		MongoCursor<Document> friendsDocs = coll.aggregate(queries).iterator();

		while (friendsDocs.hasNext()) {
			Document doc = friendsDocs.next();
			HashMap<String, ByteIterator> val = new HashMap<String, ByteIterator>();
			val.put("userid", new ObjectByteIterator(doc.getString("_id").getBytes()));

			doc.forEach((k, v) -> {
				if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
					val.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
				}
			});

			val.put("friendcount", new ObjectByteIterator(String.valueOf(doc.get(KEY_FRIEND)).getBytes()));
			result.add(val);
		}
		friendsDocs.close();
		return 0;
	}

	public List<String> listFriends(int profileOwnerID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		return coll.find(eq("_id", String.valueOf(profileOwnerID))).first().get(KEY_FRIEND, List.class);
	}

	public List<String> listPendingFriends(int profileOwnerID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		return coll.find(eq("_id", String.valueOf(profileOwnerID))).first().get(KEY_PENDING, List.class);
	}

	boolean journaled = false;

	@Override
	public boolean init() throws DBException {
		System.out.println("###init");
		if (getProperties().getProperty(KEY_MONGO_DB_IP) != null) {
			this.ipAddress = getProperties().getProperty(KEY_MONGO_DB_IP);
		}

		if (getProperties().getProperty("journaled") != null) {
			this.journaled = Boolean.parseBoolean(getProperties().getProperty("journaled"));
		}

		if (journaled) {
			this.mongoClient = new MongoClient(this.ipAddress, new MongoClientOptions.Builder()
					.serverSelectionTimeout(1000).connectionsPerHost(500).writeConcern(WriteConcern.JOURNALED).build());
		} else {
			this.mongoClient = new MongoClient(this.ipAddress,
					new MongoClientOptions.Builder().serverSelectionTimeout(1000).connectionsPerHost(500)
							.writeConcern(WriteConcern.ACKNOWLEDGED).build());
		}
		return true;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		List<Bson> list = new ArrayList<>();
		list.add(new BasicDBObject("$match",
				new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
		BasicDBList field = new BasicDBList();
		field.add("$p");
		field.add(0);
		field.add(LIST_FRIENDS);
		BasicDBObject bobj = new BasicDBObject("$project", new BasicDBObject("p", new BasicDBObject("$slice", field)));
		list.add(bobj);
		Document userProfile = coll.aggregate(list).first();
		List<String> pending = userProfile.get(KEY_PENDING, List.class);

		List<Bson> queries = new ArrayList<Bson>();
		queries.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", pending))));
		BasicDBObject obj = new BasicDBObject();
		obj.put("f", new BasicDBObject("$size", "$f"));
		obj.put("username", 1);
		obj.put("pw", 1);
		obj.put("fname", 1);
		obj.put("lname", 1);
		obj.put("gender", 1);
		obj.put("dob", 1);
		obj.put("jdate", 1);
		obj.put("ldate", 1);
		obj.put("address", 1);
		obj.put("email", 1);
		obj.put("tel", 1);
		bobj = new BasicDBObject("$project", obj);
		queries.add(bobj);
		queries.add(new BasicDBObject("$limit", LIST_FRIENDS));

		MongoCursor<Document> friendsDocs = coll.aggregate(queries).iterator();

		while (friendsDocs.hasNext()) {
			Document doc = friendsDocs.next();
			HashMap<String, ByteIterator> val = new HashMap<String, ByteIterator>();
			val.put("userid", new ObjectByteIterator(doc.getString("_id").getBytes()));
			doc.forEach((k, v) -> {
				if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
					val.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
				}
			});
			val.put("friendcount", new ObjectByteIterator(String.valueOf(doc.get(KEY_FRIEND)).getBytes()));
			results.add(val);
		}
		friendsDocs.close();
		return 0;
	}

	public int acceptFriendInviter(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		UpdateResult result = coll.updateOne(eq("_id", String.valueOf(inviterID)),
				new BasicDBObject("$addToSet", new Document(KEY_FRIEND, String.valueOf(inviteeID))));
		return 0;
	}

	/**
	 * Update user's friends and pending friends. If friends / pending friends
	 * are specified, add/remove friends / pending friends are ignored.
	 * 
	 * @param friends
	 * @param pendingFriends
	 * @param addFriends
	 * @param removeFriends
	 * @param addPendingFriends
	 * @param removePendingFriends
	 * @return
	 */
	public int updateUserDocument(String userId, Set<String> friends, Set<String> pendingFriends,
			Set<String> addFriends, Set<String> removeFriends, Set<String> addPendingFriends,
			Set<String> removePendingFriends) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		Document addToSet = new Document();
		Document pull = new Document();
		Document set = new Document();

		if (friends != null) {
			set.put(KEY_FRIEND, friends);
		} else {
			if (!isEmpty(addFriends)) {
				addToSet.put(KEY_FRIEND, new BasicDBObject("$each", addFriends));
			}
			if (!isEmpty(removeFriends)) {
				pull.put(KEY_FRIEND, new BasicDBObject("$in", removeFriends));
			}
		}

		if (pendingFriends != null) {
			set.put(KEY_PENDING, pendingFriends);
		} else {
			if (!isEmpty(addPendingFriends)) {
				addToSet.put(KEY_PENDING, new BasicDBObject("$each", addPendingFriends));
			}
			if (!isEmpty(removePendingFriends)) {
				pull.put(KEY_PENDING, new BasicDBObject("$in", removePendingFriends));
			}
		}

		BasicDBObject upsert = new BasicDBObject();
		BasicDBObject remove = new BasicDBObject();

		if (!set.isEmpty()) {
			upsert.put("$set", set);
		}

		if (!addToSet.isEmpty()) {
			upsert.put("$addToSet", addToSet);
		}

		if (!pull.isEmpty()) {
			remove.put("$pull", pull);
		}

		List<WriteModel<Document>> list = new ArrayList<>();

		if (!upsert.isEmpty()) {
			list.add(new UpdateOneModel<Document>(eq("_id", userId), upsert));
		}
		if (!pull.isEmpty()) {
			list.add(new UpdateOneModel<Document>(eq("_id", userId), remove));
		}
		// then pull from the set
		if (!list.isEmpty()) {
			log.debug("Bulk update on user " + userId + " " + upsert.toJson() + " " + remove.toJson());
			coll.bulkWrite(list);
		} else {
			log.debug("Nothing to recover for user " + userId);
		}

		return 0;
	}

	private boolean isEmpty(Collection<?> coll) {
		return coll == null || coll.isEmpty();
	}

	public int acceptFriendInvitee(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		BasicDBObject inviteeUpdate = new BasicDBObject();
		inviteeUpdate.put("$addToSet", new Document(KEY_FRIEND, String.valueOf(inviterID)));
		inviteeUpdate.put("$pull", new Document(KEY_PENDING, String.valueOf(inviterID)));
		coll.updateOne(eq("_id", String.valueOf(inviteeID)), inviteeUpdate);
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		acceptFriendInviter(inviterID, inviteeID);
		acceptFriendInvitee(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		coll.updateOne(eq("_id", String.valueOf(inviteeID)),
				new BasicDBObject("$pull", new Document(KEY_PENDING, String.valueOf(inviterID))));
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		coll.updateOne(eq("_id", String.valueOf(inviteeID)),
				new BasicDBObject("$addToSet", new Document(KEY_PENDING, String.valueOf(inviterID))));
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int thawFriendInviter(int friendid1, int friendid2) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		coll.updateOne(eq("_id", String.valueOf(friendid1)),
				new BasicDBObject("$pull", new Document(KEY_FRIEND, String.valueOf(friendid2))));
		return 0;
	}

	public int thawFriendInvitee(int friendid1, int friendid2) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		coll.updateOne(eq("_id", String.valueOf(friendid2)),
				new BasicDBObject("$pull", new Document(KEY_FRIEND, String.valueOf(friendid1))));
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		thawFriendInviter(friendid1, friendid2);
		thawFriendInvitee(friendid1, friendid2);
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		HashMap<String, String> stats = new HashMap<>();
		System.out.println("initialized users " + coll.count());
		stats.put("usercount", String.valueOf(coll.count()));
		stats.put("resourcesperuser", "0");
		stats.put("avgfriendsperuser", String.valueOf(coll.find().first().get(KEY_FRIEND, List.class).size()));
		stats.put("avgpendingperuser", String.valueOf(coll.find().first().get(KEY_PENDING, List.class).size()));
		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		insert(friendid1, friendid2);
		insert(friendid2, friendid1);
		return 0;
	}

	private void insert(int friendid1, int friendid2) {
		Set<String> friends = this.friendList.get(friendid1);
		if (friends == null) {
			friends = new HashSet<>();
			this.friendList.put(friendid1, friends);
		}
		friends.add(String.valueOf(friendid2));
	}

	@Override
	public void createSchema(Properties props) {
		mongoClient.dropDatabase(MONGO_DB_NAME);
		MongoDatabase db = mongoClient.getDatabase(MONGO_DB_NAME);
		db.createCollection(MONGO_USER_COLLECTION);
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		System.out.println("###clean");
		bulkWriteFriends();
		mongoClient.close();
		super.cleanup(warmup);
	}

	public void bulkWriteFriends() {
		if (!this.friendList.isEmpty()) {

			MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_USER_COLLECTION);

			List<WriteModel<Document>> list = new ArrayList<>();
			this.friendList.forEach((id, friends) -> {
				list.add(new UpdateOneModel<Document>(eq("_id", String.valueOf(id)),
						new BasicDBObject("$set", new Document(KEY_FRIEND, friends))));
			});
			coll.bulkWrite(list);
			this.friendList.clear();
		}
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		System.out.println("queryPendingFriendshipIds");
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		System.out.println("queryConfirmedFriendshipIds");
		return 0;
	}
}