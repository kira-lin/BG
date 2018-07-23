/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
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

package memcached;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;


import common.CacheUtilities;


/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with BG.
 * This class extends {@link DB} and implements the database interface used by BG client.
 *
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 *
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore,
 * only one index on the primary key is needed.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * 
 *
 */


public class ClientForRefreshTechniqueOptimization extends DB implements JdbcDBMemCachedClientConstants {
	protected static String FSimagePath = "";
	protected static boolean initialized = false;
	protected static boolean sockPoolInitialized = false;
	protected Properties props;
	protected static final String DEFAULT_PROP = "";
	protected ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	protected PreparedStatement preparedStatement;
	protected boolean verbose = false;
	protected Connection conn = null;
	protected static int GETFRNDCNT_STMT = 2;
	protected static int GETPENDCNT_STMT = 3;
	protected static int GETRESCNT_STMT = 4;
	protected static int GETPROFILE_STMT = 5;
	protected static int GETPROFILEIMG_STMT = 6;
	protected static int GETFRNDS_STMT = 7;
	protected static int GETFRNDSIMG_STMT = 8;
	protected static int GETPEND_STMT = 9;
	protected static int GETPENDIMG_STMT = 10;
	protected static int REJREQ_STMT = 11;
	protected static int ACCREQ_STMT = 12;
	protected static int INVFRND_STMT = 13;
	protected static int UNFRNDFRND_STMT = 14;
	protected static int GETTOPRES_STMT = 15;
	protected static int GETRESCMT_STMT = 16;
	protected static int POSTCMT_STMT = 17;
	protected static int DELCMT_STMT = 18;

	protected static boolean ManageCache = false;
	protected static final int CACHE_START_WAIT_TIME = 10000;
	//protected static Vector<MemcachedClient> cacheclient_vector = new Vector<MemcachedClient>();
	static SockIOPool cacheConnectionPool = null;
	protected MemcachedClient cacheclient = null;
	protected static String cache_hostname = "";
	protected static Integer cache_port = -1;
	protected boolean isInsertImage;
	StartProcess st = null;

	protected byte[] deserialize_buffer = null;

	protected static int listener_port = 11111;
	protected static boolean USE_LISTENER_START_CACHE = true;
	protected static boolean cleanup_connectionpool = true;

	protected static final int MAX_NUM_RETRIES = 10;
	protected static final int TIMEOUT_WAIT_MILI = 100;

	//String cache_cmd = "C:\\cosar\\conficacheclientle\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
	public static final int CACHE_POOL_NUM_CONNECTIONS = 400;
	protected static final String CACHE_POOL_NAME = "MEMCACHEDBGPOOL";

	protected static AtomicInteger NumThreads = null;
	protected static Semaphore crtcl = new Semaphore(1, true);

	protected static final double TTL_RANGE_PERCENT = 0.2; 
	protected boolean useTTL = false;
	protected int TTLvalue = 0;
	protected boolean compressPayload = false;

	protected static int GETUSER_STMT = 19;
	protected static int GETUSERIMG_STMT = 20;

	protected String getKeyListFriends(int userID) {
		return isInsertImage == true ? "lsFrds:" + userID : "lsFrdsNoImage:" + userID;  
	}

	protected String getKeyProfile(int userID) {
		return isInsertImage == true ? "profile" + userID : "profileNoImage" + userID;  
	}

	protected String getKeyFriendRequests(int userID) {
		return isInsertImage == true ? "viewPendReq:" + userID : "viewPendReqNoImage:" + userID;
	}

	protected String getKeyCommentsofResource(int resID) {
		return "ResCmts:"+ resID;
	}

	protected String getKeyTopKResoures(int userID) {
		return "TopKRes:" + userID;
	}

	private boolean updateCache(String key, byte[] payload) {
		try {
			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
			{
				System.out.println("Error in updateCache. Failed to insert");
				return false;
			}

			return true;
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			return false;
		}				
	}

	protected KeyValuesObject updateProfileFriendInvite(int id) {
		if (id < 0)
			return null;

		String key = getKeyProfile(id);		
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (!CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)) {
			System.out.println("Error in OwnProfilePending: Failed to unMarshallVectorOfHashMaps");
			return null;
		}
		
		int currValue = Integer.parseInt(new String(result.get("pendingcount").toArray()));
		currValue += 1;
		result.put("pendingcount", new ObjectByteIterator((currValue + "").getBytes()));

		kvObj.setNewValue(CacheUtilities.SerializeHashMap(result));

		return kvObj;
	}

	protected KeyValuesObject updateProfileFriendReject(int id) {
		if (id < 0)
			return null;

		String key = getKeyProfile(id);

		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (!CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)) {
			System.out.println("Error in OwnProfilePending: Failed to unMarshallVectorOfHashMaps");
			return null;
		}
		
		int currValue = Integer.parseInt(new String(result.get("pendingcount").toArray()));
		currValue -= 1;
		result.put("pendingcount", new ObjectByteIterator((currValue + "").getBytes()));		

		kvObj.setNewValue(CacheUtilities.SerializeHashMap(result));

		return kvObj;
	}

	protected KeyValuesObject updateInviterProfileFriendAccept(int id) {
		if (id < 0)
			return null;

		String key = getKeyProfile(id);		
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (!CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)) {
			System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
			return null;
		}
		
		int currValue = Integer.parseInt(new String(result.get("friendcount").toArray()));
		currValue += 1;
		result.put("friendcount", new ObjectByteIterator((currValue + "").getBytes()));	

		kvObj.setNewValue(CacheUtilities.SerializeHashMap(result));

		return kvObj;
	}

	protected KeyValuesObject updateInviteeProfileFriendAccept(int id) {
		if (id < 0)
			return null;

		String key = getKeyProfile(id);		
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (!CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)) {
			System.out.println("Error in OwnProfilePending: Failed to unMarshallVectorOfHashMaps");
			return null;
		}		

		int currValue = Integer.parseInt(new String(result.get("friendcount").toArray()));
		currValue += 1;
		result.put("friendcount", new ObjectByteIterator((currValue + "").getBytes()));
		
		currValue = Integer.parseInt(new String(result.get("pendingcount").toArray()));
		currValue -= 1;
		result.put("pendingcount", new ObjectByteIterator((currValue + "").getBytes()));	

		kvObj.setNewValue(CacheUtilities.SerializeHashMap(result));

		return kvObj;
	}

	protected KeyValuesObject updateProfileFriendThaw(int id) {
		if (id < 0)
			return null;

		String key = getKeyProfile(id);

		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		if (!CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)) {
			System.out.println("Error in OwnProfilePending: Failed to unMarshallVectorOfHashMaps");
			return null;
		}
		
		int currValue = Integer.parseInt(new String(result.get("friendcount").toArray()));
		currValue -= 1;
		result.put("friendcount", new ObjectByteIterator((currValue + "").getBytes()));		

		kvObj.setNewValue(CacheUtilities.SerializeHashMap(result));

		return kvObj;		
	}	

	protected KeyValuesObject updatePendingFriendsRequest(int inviterID, int inviteeID) {
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();;

		HashMap<String, ByteIterator> user1ProfileValues = new HashMap<String, ByteIterator>();		
		if (viewProfile(inviteeID, inviterID, user1ProfileValues, isInsertImage, false) == 0) {
			values.put("userid", user1ProfileValues.get("USERID"));
			values.put("INVITERID", user1ProfileValues.get("USERID"));
			values.put("INVITEEID", new ObjectByteIterator((inviteeID + "").getBytes()));

			values.put("USERNAME", user1ProfileValues.get("USERNAME"));
			values.put("FNAME", user1ProfileValues.get("FNAME"));
			values.put("LNAME", user1ProfileValues.get("LNAME"));
			values.put("GENDER", user1ProfileValues.get("GENDER"));
			values.put("DOB", user1ProfileValues.get("DOB"));
			values.put("JDATE", user1ProfileValues.get("JDATE"));
			values.put("LDATE", user1ProfileValues.get("LDATE"));
			values.put("ADDRESS", user1ProfileValues.get("ADDRESS"));
			values.put("EMAIL", user1ProfileValues.get("EMAIL"));
			values.put("TEL", user1ProfileValues.get("TEL"));

			if (isInsertImage)
				values.put("tpic", user1ProfileValues.get("pic"));				
		} else {
			System.out.println("Error viewProfile for updatePendingFriendsInvite: userid= " + inviterID);
			return null;
		}

		String key = getKeyFriendRequests(inviteeID);
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in updatePendingFriendsInvite: Failed to unMarshallVectorOfHashMaps " + inviterID + ": " + inviteeID);
			return null;
		}

		result.add(values);		
		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	protected KeyValuesObject updatePendingFriendsRemove(int inviterID, int inviteeID) {
		String key = getKeyFriendRequests(inviteeID);		
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);		
		if (payload == null)
			return kvObj;		

//		kvObj.setOldValue(payload);

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
			return null;
		}

		for (int i = 0; i < result.size(); i++) {
			HashMap<String, ByteIterator> friendEntry = result.get(i);

			int id = Integer.parseInt(new String(friendEntry.get("userid").toArray()));
			if (id == inviterID) {
				result.remove(friendEntry);
				break;
			}

			friendEntry.put("userid", new ObjectByteIterator((id + "").getBytes()));
		}

		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	protected KeyValuesObject updateListFriendsAccept(int user1, int user2) {
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();;

		HashMap<String, ByteIterator> user2ProfileValues = new HashMap<String, ByteIterator>();		
		if (viewProfile(user1, user2, user2ProfileValues, isInsertImage, false) == 0) {
			values.put("userid", new ObjectByteIterator((user2 + "").getBytes()));
			values.put("INVITERID", new ObjectByteIterator((user1 + "").getBytes()));
			values.put("INVITEEID", new ObjectByteIterator((user2 + "").getBytes()));

			values.put("USERNAME", user2ProfileValues.get("USERNAME"));
			values.put("FNAME", user2ProfileValues.get("FNAME"));
			values.put("LNAME", user2ProfileValues.get("LNAME"));
			values.put("GENDER", user2ProfileValues.get("GENDER"));
			values.put("DOB", user2ProfileValues.get("DOB"));
			values.put("JDATE", user2ProfileValues.get("JDATE"));
			values.put("LDATE", user2ProfileValues.get("LDATE"));
			values.put("ADDRESS", user2ProfileValues.get("ADDRESS"));
			values.put("EMAIL", user2ProfileValues.get("EMAIL"));
			values.put("TEL", user2ProfileValues.get("TEL"));

			if (isInsertImage)
				values.put("tpic", user2ProfileValues.get("pic"));				
		} else {
			System.out.println("Error viewProfile for updatePendingFriendsInvite: user2id= " + user2);
			return null;
		}

		String key = getKeyListFriends(user1);
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in updatePendingFriendsInvite: Failed to unMarshallVectorOfHashMaps " + user1 + ": " + user2);
			return null;
		}

		result.add(values);
		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	protected KeyValuesObject updateListFriendsRemove(int user1, int user2) {
		String key = getKeyListFriends(user1);
		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in updateListFriendsRemove: Failed to unMarshallVectorOfHashMaps");
			return null;
		}

		for (int i= 0; i < result.size(); i++) {
			HashMap<String, ByteIterator> entry = result.get(i);

			int id = Integer.parseInt(new String(entry.get("userid").toArray()));
			if (id == user2) {
				result.remove(entry);
				break;
			}

			entry.put("userid", new ObjectByteIterator((id + "").getBytes()));
		}

		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	protected KeyValuesObject addComment(int resID, int commentCreatorID, int profileOwnerID, HashMap<String, ByteIterator> commentValues) {
		String key = getKeyCommentsofResource(resID);

		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

//		kvObj.setOldValue(payload);

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
			return null;
		}

		HashMap<String, ByteIterator> commentEntry = new HashMap<String, ByteIterator>();
		commentEntry.put("MID", commentValues.get("mid"));
		commentEntry.put("CREATORID", new ObjectByteIterator((profileOwnerID + "").getBytes()));
		commentEntry.put("RID", new ObjectByteIterator((resID + "").getBytes()));
		commentEntry.put("MODIFIERID", new ObjectByteIterator((commentCreatorID + "").getBytes()));
		commentEntry.put("TIMESTAMP", commentValues.get("timestamp"));
		commentEntry.put("TYPE", commentValues.get("type"));
		commentEntry.put("CONTENT", commentValues.get("content"));

		result.add(commentEntry);

		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	protected KeyValuesObject deleteCommentOnResoure(int resCreatorID, int resID, int manipulationID) {
		String key = getKeyCommentsofResource(resID);

		byte[] payload = common.CacheUtilities.CacheGet(cacheclient, key, this.compressPayload);

		KeyValuesObject kvObj = new KeyValuesObject(key);

		if (payload == null)
			return kvObj;

		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();		
		if (!CacheUtilities.unMarshallVectorOfHashMaps(payload, result, this.deserialize_buffer)) {
			System.out.println("Error in resCommentRemove (Failed to unMarshallVectorOfHashMaps), resID=" + resID + ", manipulationID=" + manipulationID);
			return null;
		}

		for (int i = 0; i < result.size(); i++) {
			HashMap<String, ByteIterator> entry = result.get(i);

			int id = Integer.parseInt(new String(entry.get("MID").toArray()));
			if (id == manipulationID) {
				result.removeElement(entry);

				break;
			}

			entry.put("MID", new ObjectByteIterator((id + "").getBytes()));

		}

		kvObj.setNewValue(CacheUtilities.SerializeVectorOfHashMaps(result));

		return kvObj;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;

		if(inviterID < 0 || inviteeID < 0)
			return -1;

		CallableStatement proc = null;
		try {
			conn.setAutoCommit(false);
			proc = conn.prepareCall("{ call INVITEFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
			proc.setInt(2, inviteeID);
			proc.execute();

			if (!useTTL)
			{
				KeyValuesObject kvo1 = updateProfileFriendInvite(inviteeID);
				if (kvo1 == null || (kvo1.getNewValue() != null && !updateCache(kvo1.getKey(), kvo1.getNewValue()))) {
					conn.rollback();
					return -2;
				}

				KeyValuesObject kvo2 = updatePendingFriendsRequest(inviterID, inviteeID);
				if (kvo2 == null || (kvo2.getNewValue() != null && !updateCache(kvo2.getKey(), kvo2.getNewValue()))) {
					conn.rollback();
					return -2;		    	
				}

				conn.commit();
				return SUCCESS;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}catch (Exception e1) {
			e1.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				conn.setAutoCommit(true);
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);

			}
		}
		return retVal;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;

		CallableStatement proc = null;
		try {
			conn.setAutoCommit(false);
			proc = conn.prepareCall("{ call REJECTFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
			proc.setInt(2, inviteeID);
			proc.execute();

			if (!useTTL)
			{
				KeyValuesObject kvo = updateProfileFriendReject(inviteeID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				kvo = updatePendingFriendsRemove(inviterID, inviteeID);	
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;		
				}

				return SUCCESS;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}catch (Exception e1) {
			e1.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				conn.setAutoCommit(true);
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;	
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;

		if(inviterID < 0 || inviteeID < 0)
			return -1;

		CallableStatement proc = null;
		try {
			conn.setAutoCommit(false);
			
			// update DBMS
			proc = conn.prepareCall("{ call ACCEPTFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
			proc.setInt(2, inviteeID);
			proc.execute();

			if(!useTTL)
			{
				// update pending friends of invitee
				KeyValuesObject kvo = updatePendingFriendsRemove(inviterID, inviteeID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				// update profile
				kvo = updateInviterProfileFriendAccept(inviterID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				kvo = updateInviteeProfileFriendAccept(inviteeID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				// update list friends of inviter and invitee
				kvo = updateListFriendsAccept(inviterID, inviteeID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				kvo = updateListFriendsAccept(inviteeID, inviterID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				return SUCCESS;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}catch (Exception e1) {
			e1.printStackTrace(System.out);
			retVal = -2;
		}finally {
			try {
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;		
	}	

	@Override
	public int thawFriendship(int friendid1, int friendid2){
		int retVal = SUCCESS;

		if(friendid1 < 0 || friendid2 < 0)
			return -1;

		CallableStatement proc = null;
		try {
			conn.setAutoCommit(false);
			proc = conn.prepareCall("{ call THAWFRIEND(?, ?) }");
			proc.setInt(1, friendid1);
			proc.setInt(2, friendid2);
			proc.execute();	    	

			if (!useTTL)
			{
				// update profile 
				KeyValuesObject kvo = updateProfileFriendThaw(friendid1);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				kvo = updateProfileFriendThaw(friendid2);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				// update list friends 
				kvo = updateListFriendsRemove(friendid1, friendid2);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				kvo = updateListFriendsRemove(friendid2, friendid1);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				return SUCCESS;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}catch (Exception e1) {
			e1.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				conn.setAutoCommit(true);
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> commentValues) {
		int retVal = SUCCESS;

		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;

		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?,?,?, ?,?, ?, ?)";
		try {
			conn.setAutoCommit(false);
			
			if((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);	
			preparedStatement.setInt(1, Integer.parseInt(commentValues.get("mid").toString()));
			preparedStatement.setInt(2, profileOwnerID);
			preparedStatement.setInt(3, resourceID);
			preparedStatement.setInt(4,commentCreatorID);
			preparedStatement.setString(5,commentValues.get("timestamp").toString());
			preparedStatement.setString(6,commentValues.get("type").toString());
			preparedStatement.setString(7,commentValues.get("content").toString());
			preparedStatement.executeUpdate();

			if (!useTTL)
			{
				KeyValuesObject kvo = addComment(resourceID, commentCreatorID, profileOwnerID, commentValues);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				return SUCCESS;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				conn.setAutoCommit(true);
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		int retVal = SUCCESS;

		if(resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
			return -1;

		String query = "DELETE FROM manipulation WHERE mid=? AND rid=?";
		try {
			conn.setAutoCommit(false);
			
			if((preparedStatement = newCachedStatements.get(DELCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(DELCMT_STMT, query);	
			preparedStatement.setInt(1, manipulationID);
			preparedStatement.setInt(2, resourceID);
			preparedStatement.executeUpdate();

			if (!useTTL) {
				KeyValuesObject kvo = deleteCommentOnResoure(resourceCreatorID, resourceID, manipulationID);
				if (kvo == null || ((kvo.getNewValue() != null && !updateCache(kvo.getKey(), kvo.getNewValue())))) {
					conn.rollback();
					return -2;
				}

				return SUCCESS;
			}


		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				conn.setAutoCommit(true);
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}	

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		ResultSet rs = null;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String key = getKeyListFriends(profileOwnerID);

		// Check Cache first
		try {
			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
			if (payload != null){

				//				System.out.println("Cache get: " + md5Java(payload));

				if (verbose) System.out.println("... Cache Hit!");
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result, this.deserialize_buffer))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");

				//cacheclient.shutdown();
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to serialize a vector of hashmaps.");
			retVal = -2;
		}

		String query = "";
		try {
			if(insertImage){
				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE (inviterid=? and userid=inviteeid) and status = 2";
				if((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);	

			}else{
				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE (inviterid=? and userid=inviteeid) and status = 2";
				if((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);	
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt =0;
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				if (fields != null) {
					for (String field : fields) {
						String value = rs.getString(field);
						if(field.equalsIgnoreCase("userid"))
							field = "userid";

						values.put(field, new ObjectByteIterator(value.getBytes()));
					}
					result.add(values);
				}else{
					//get the number of columns and their names
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++){
						String col_name = md.getColumnName(i);
						String value="";
						if(col_name.equalsIgnoreCase("tpic")){
							//Get as a BLOB
							Blob aBlob = rs.getBlob(col_name);
							byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
							if(testMode){
								//dump to file
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								}catch(Exception ex){
								}
							}
							values.put(col_name, new ObjectByteIterator(allBytesInBlob));
						}else{
							value = rs.getString(col_name);
							if(col_name.equalsIgnoreCase("userid")){
								col_name = "userid";
							}
							values.put(col_name, new ObjectByteIterator(value.getBytes()));
						}

					}

					result.add(values);
				}
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		//serialize the result hashmap and insert it in the cache for future use

		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);

		//		System.out.println("Cache set: " + md5Java(payload));

		try {
			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
			{
				//throw new Exception("Error calling WhalinMemcached set");
			}
			//cacheclient.shutdown();
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}

		return retVal;		
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0)
			return -1;

		String query = "";

		String key = getKeyFriendRequests(profileOwnerID);			

		// Check Cache first
		try {
			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
			if (payload != null){
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result, this.deserialize_buffer))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
				if (verbose) System.out.println("... Cache Hit!");
				//				System.out.println("Cache get: " + md5Java(payload));
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
			retVal = -2;
		}

		try {
			if(insertImage){
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				if((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);	

			}else {
				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				if((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);		
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt=0;
			while (rs.next()) {
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = "";
					if(col_name.equalsIgnoreCase("tpic")){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}

						}
						values.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else{
						value = rs.getString(col_name);
						if(col_name.equalsIgnoreCase("userid")){
							col_name = "userid";
						}
						values.put(col_name, new ObjectByteIterator(value.getBytes()));
					}					
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
		//		System.out.println("Cache set: " + md5Java(payload));		

		try {
			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
			{
				//throw new Exception("Error calling WhalinMemcached set");
			}
			//cacheclient.shutdown();
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}


		return retVal;		
	}

	@Override
	//Load phase query not cached
	public int CreateFriendship(int memberA, int memberB) {
		int retVal = SUCCESS;
		if(memberA < 0 || memberB < 0)
			return -1;
		CallableStatement proc = null;
		try {
			proc = conn.prepareCall("{ call INSERTFRIEND(?, ?) }");
			proc.setInt(1, memberA);
			proc.setInt(2, memberB);
			proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0 || requesterID < 0 || k < 0)
			return -1;

		String key = "TopKRes:"+profileOwnerID;
		//MemcachedClient cacheclient=null;
		// Check Cache first
		try {
			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
			if (payload != null && CacheUtilities.unMarshallVectorOfHashMaps(payload,result, this.deserialize_buffer)){
				if (verbose) System.out.println("... Cache Hit!");
				//cacheclient.shutdown();
				return retVal;
			}
			else if (verbose) System.out.print("... Query DB!");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			return -2;
		}

		String query = "SELECT * FROM resources WHERE walluserid = ? AND rownum <? ORDER BY rid desc";
		try {
			if((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);		

			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k+1));
			rs = preparedStatement.executeQuery();
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if(col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					else if(col_name.equalsIgnoreCase("walluserid"))
						col_name = "walluserid";
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		if (retVal == SUCCESS){
			//serialize the result hashmap and insert it in the cache for future use
			byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
			try {
				if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
				{
					//throw new Exception("Error calling WhalinMemcached set");
				}
				//cacheclient.shutdown();
			} catch (Exception e1) {
				System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
				e1.printStackTrace(System.out);
				retVal = -2;
			}
		}

		return retVal;		
	}

	public int getCreatedResources(int resourceCreatorID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		Statement st = null;
		if(resourceCreatorID < 0)
			return -1;

		String query = "SELECT * FROM resources WHERE creatorid = "+resourceCreatorID;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if(col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {

		ResultSet rs = null;
		int retVal = SUCCESS;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query="";
		String uid="";

		byte[] payload;
		String key;

		//MemcachedClient cacheclient=null;
		HashMap<String, ByteIterator> SR = new HashMap<String, ByteIterator>(); 

		// Initialize query logging for the send procedure
		//cacheclient.startQueryLogging();

		// Check Cache first
		key = getKeyProfile(profileOwnerID);
		try {
			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
			payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
			if (payload != null && CacheUtilities.unMarshallHashMap(result, payload, this.deserialize_buffer)){
				if (verbose) System.out.println("... Cache Hit!");

				//cacheclient.shutdown();
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			return -2;
		} 

		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic, pendcount, confcount, rescount FROM  users WHERE UserID = ?";
				if((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);	
			}else{
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pendcount, confcount, rescount FROM  users WHERE UserID = ?";
				if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);	
			}

			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if(rs.next()){
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value ="";

					if (col_name.equalsIgnoreCase("userid")){
						uid = rs.getString(col_name);
					}
					if(col_name.equalsIgnoreCase("pic") ){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						//if test mode dump pic into a file
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-proimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}
						}
						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
						SR.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else if (col_name.equalsIgnoreCase("rescount")){
						result.put("resourcecount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
						SR.put("resourcecount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else if(col_name.equalsIgnoreCase("pendcount")){
						result.put("pendingcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
						SR.put("pendingcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else if(col_name.equalsIgnoreCase("confcount")){
						result.put("friendcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
						SR.put("friendcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else{
						value = rs.getString(col_name);
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
						SR.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
				}

				//Fetch the profile image from the file system
				if (insertImage && !FSimagePath.equals("") ){
					//Get the profile image from the file
					byte[] profileImage = common.RdbmsUtilities.GetImageFromFS(uid, true, FSimagePath);
					if(testMode){
						//dump to file
						try{
							FileOutputStream fos = new FileOutputStream(profileOwnerID+"-proimage.bmp");
							fos.write(profileImage);
							fos.close();
						}catch(Exception ex){
						}
					}
					result.put("pic", new ObjectByteIterator(profileImage) );
					SR.put("pic", new ObjectByteIterator(profileImage) );
				}
			}

		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		//serialize the result hashmap and insert it in the cache for future use
		payload = CacheUtilities.SerializeHashMap(SR);
		try {
			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
			{
				//throw new Exception("Error calling WhalinMemcached set");
			}
			//while(setResult.isDone() == false);
			//cacheclient.shutdown();
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}

		return retVal;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;

		String key = getKeyCommentsofResource(resourceID);
		String query="";
		//MemcachedClient cacheclient=null;
		// Check Cache first
		try {
			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
			if (payload != null){
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result, this.deserialize_buffer))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");

				if (verbose) System.out.println("... Cache Hit!");
				//cacheclient.shutdown();

				//				System.out.println("Cache get: " + md5Java(payload));				
				return retVal;
			} else if (verbose) System.out.print("... Query DB!");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
			retVal = -2;
		}

		//get comment cnt
		try {	
			query = "SELECT * FROM manipulation WHERE rid = ?";		
			if((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);		
			preparedStatement.setInt(1, resourceID);
			rs = preparedStatement.executeQuery();
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
		//		System.out.println("Cache set: " + md5Java(payload));		
		try {			
			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
			{
				//throw new Exception("Error calling WhalinMemcached set");
			}
			//cacheclient.shutdown();
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}

		return retVal;		
	}

	@Override
	//init phase query not cached
	public HashMap<String, String> getInitialStats() {
		return common.RdbmsUtilities.getInitialStats2R1T(conn);
		//		HashMap<String, String> stats = new HashMap<String, String>();
		//		Statement st = null;
		//		ResultSet rs = null;
		//		String query = "";
		//		try {
		//			st = conn.createStatement();
		//			//get user count
		//			query = "SELECT count(*) from users";
		//			rs = st.executeQuery(query);
		//			if(rs.next()){
		//				stats.put("usercount",rs.getString(1));
		//			}else
		//				stats.put("usercount","0"); //sth is wrong - schema is missing
		//			if(rs != null ) rs.close();
		//			
		//			//get resources per user
		//			query = "SELECT avg(rescount) from users";
		//			rs = st.executeQuery(query);
		//			if(rs.next()){
		//				stats.put("resourcesperuser",rs.getString(1));
		//			}else{
		//				stats.put("resourcesperuser","0");
		//			}
		//			if(rs != null) rs.close();	
		//			//get number of friends per user
		//			query = "select avg(confcount) from users" ;
		//			rs = st.executeQuery(query);
		//			if(rs.next()){
		//				stats.put("avgfriendsperuser",rs.getString(1));
		//			}else
		//				stats.put("avgfriendsperuser","0");
		//			if(rs != null) rs.close();
		//			query = "select avg(pendcount) from users" ;
		//			rs = st.executeQuery(query);
		//			if(rs.next()){
		//				stats.put("avgpendingperuser",rs.getString(1));
		//			}else
		//				stats.put("avgpendingperuser","0");
		//			
		//
		//		}catch(SQLException sx){
		//			sx.printStackTrace(System.out);
		//		}finally{
		//			try {
		//				if(rs != null)
		//					rs.close();
		//				if(st != null)
		//					st.close();
		//			} catch (SQLException e) {
		//				e.printStackTrace(System.out);
		//			}
		//
		//		}
		//		return stats;
	}

	//init phase query not cached
	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(inviteeid < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid from friendship where inviteeid='"+inviteeid+"' and status='1'";
			rs = st.executeQuery(query);
			while(rs.next()){
				pendingIds.add(rs.getInt(1));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;
	}

	//init phase query not cached
	public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(profileId < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid, inviteeid from friendship where inviterid="+profileId+" and status='2'";
			rs = st.executeQuery(query);
			while(rs.next()){
				if(rs.getInt(1) != profileId)
					confirmedIds.add(rs.getInt(1));
				else
					confirmedIds.add(rs.getInt(2));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;

	}

	protected String getCacheCmd()
	{		
		return "C:\\PSTools\\psexec \\\\"+cache_hostname+" -u shahram -p 2Shahram C:\\memcached\\memcached.exe -d start ";
	}

	protected String getCacheStopCmd()
	{
		return "C:\\PSTools\\psexec \\\\"+cache_hostname+" -u shahram -p 2Shahram C:\\memcached\\memcached.exe -d stop ";
	}




	protected static int incrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v + 1));
		return v + 1;
	}

	protected static int decrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v - 1));
		return v - 1;
	}


	protected void cleanupAllConnections() {
		try {
			//close all cached prepare statements
			Set<Integer> statementTypes = newCachedStatements.keySet();
			Iterator<Integer> it = statementTypes.iterator();
			while(it.hasNext()){
				int stmtType = it.next();
				if(newCachedStatements.get(stmtType) != null) newCachedStatements.get(stmtType).close();
			}
			newCachedStatements.clear();
			if(conn != null) {
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 * 
	 */
	@Override
	public boolean init() throws DBException {
		//		if (initialized) {
		//			System.out.println("Client connection already initialized.");
		//			return true;
		//		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);
		FSimagePath = props.getProperty(FS_PATH, DEFAULT_PROP);

		cache_hostname = props.getProperty(MEMCACHED_SERVER_HOST, MEMCACHED_SERVER_HOST_DEFAULT);
		cache_port = Integer.parseInt(props.getProperty(MEMCACHED_SERVER_PORT, MEMCACHED_SERVER_PORT_DEFAULT));

		ManageCache = Boolean.parseBoolean(
				props.getProperty(MANAGE_CACHE_PROPERTY, 
						MANAGE_CACHE_PROPERTY_DEFAULT));

		isInsertImage = Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		TTLvalue = Integer.parseInt(props.getProperty(TTL_VALUE, TTL_VALUE_DEFAULT));
		useTTL = (TTLvalue != 0);

		compressPayload = Boolean.parseBoolean(props.getProperty(ENABLE_COMPRESSION_PROPERTY, ENABLE_COMPRESSION_PROPERTY_DEFAULT));

		listener_port = Integer.parseInt(props.getProperty(common.CacheClientConstants.LISTENER_PORT, common.CacheClientConstants.LISTENER_PORT_DEFAULT));
		USE_LISTENER_START_CACHE = props.getProperty(common.CacheClientConstants.LISTENER_PORT) != null;

		cleanup_connectionpool = Boolean.parseBoolean(props.getProperty(CLEANUP_CACHEPOOL_PROPERTY, CLEANUP_CACHEPOOL_PROPERTY_DEFAULT));		

		deserialize_buffer = new byte[common.CacheUtilities.deserialize_buffer_size];

		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url: urls.split(",")) {
				if(conn == null) {
					conn = DriverManager.getConnection(url, user, passwd);
					// Since there is no explicit commit method in the DB interface, all
					// operations should auto commit.
					conn.setAutoCommit(true);
				} else {
					System.out.println("Warning: init called when conn was not null");
				}
			}
			newCachedStatements = new ConcurrentHashMap<Integer, PreparedStatement>();
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBS driver: " + e);
			e.printStackTrace(System.out);
			return false;
		} catch (SQLException e) {
			System.out.println("Error in database operation: " + e);
			e.printStackTrace(System.out);
			return false;
		} catch (NumberFormatException e) {
			System.out.println("Invalid value for fieldcount property. " + e);
			e.printStackTrace(System.out);
			return false;
		}


		try {
			crtcl.acquire();

			if(NumThreads == null)
			{
				NumThreads = new AtomicInteger();
				NumThreads.set(0);
			}

			incrementNumThreads();

		}catch (Exception e){
			System.out.println("init failed to acquire semaphore.");
			e.printStackTrace(System.out);
		}
		if (initialized) {
			cacheclient = new MemcachedClient(CACHE_POOL_NAME);			
			//cacheclient.setMaxStaleness(max_staleness);
			//cacheclient_vector.add(cacheclient);
			cacheclient.setCompressEnable(false);
			//cacheclient.setMaxObjectSize(common.CacheUtilities.deserialize_buffer_size);
			crtcl.release();
			//System.out.println("Client connection already initialized.");
			return true;
		}

		//useTTL = Integer.parseInt(props.getProperty("ttlvalue", "0")) != 0;

		Properties prop = new Properties();
		prop.setProperty("log4j.rootLogger", "ERROR, A1");
		//prop.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
		prop.setProperty("log4j.appender.A1", "org.apache.log4j.FileAppender");
		prop.setProperty("log4j.appender.A1.File", "whalincache_error.out");
		prop.setProperty("log4j.appender.A1.Append", "false");
		prop.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
		prop.setProperty("log4j.appender.A1.layout.ConversionPattern", "%-4r %-5p [%t] %37c %3x - %m%n");
		PropertyConfigurator.configure(prop);

		//String[] servers = { "192.168.1.1:1624", "192.168.1.1:1625" };


		if (ManageCache) {

			System.out.println("Starting Cache: "+this.getCacheCmd() + ";" + new Date().toString());

			if(USE_LISTENER_START_CACHE)
			{
				//common.CacheUtilities.runListener(cache_hostname, listener_port, "C:\\memcached\\memcached.exe -d start");
				common.CacheUtilities.startMemcached(cache_hostname, listener_port);
			}
			else
			{
				//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
				this.st = new StartProcess(this.getCacheCmd(), "cache_output.txt");
				this.st.start();
			}

			System.out.println("Wait for "+CACHE_START_WAIT_TIME/1000+" seconds to allow Cache to startup.");
			try{
				Thread.sleep(CACHE_START_WAIT_TIME);
			}catch(Exception e)
			{
				e.printStackTrace(System.out);
			}
		}


		String[] servers = { cache_hostname + ":" + cache_port };
		if(sockPoolInitialized) {
			//			SockIOPool.getInstance( CACHE_POOL_NAME ).shutDown();
			//			sockPoolInitialized = false;
			//			
			//			try {
			//				System.out.println("Waiting for cache connection pool to shutdown..." + new Date().toString());
			//				Thread.sleep(CACHE_START_WAIT_TIME);
			//			} catch (InterruptedException e) {
			//				// TODO Auto-generated catch block
			//				e.printStackTrace();
			//			}
		}

		if(!sockPoolInitialized) 
		{
			cacheConnectionPool = SockIOPool.getInstance( CACHE_POOL_NAME );
			cacheConnectionPool.setServers( servers );
			cacheConnectionPool.setFailover( true );
			cacheConnectionPool.setInitConn( 10 ); 
			cacheConnectionPool.setMinConn( 5 );
			cacheConnectionPool.setMaxConn( CACHE_POOL_NUM_CONNECTIONS );
			//			cacheConnectionPool.setInitConn( 1 ); 
			//			cacheConnectionPool.setMinConn( 1 );
			//			cacheConnectionPool.setMaxConn( 1 );			
			cacheConnectionPool.setMaintSleep( 30 );
			cacheConnectionPool.setNagle( false );
			cacheConnectionPool.setSocketTO( 3000 );
			cacheConnectionPool.setAliveCheck( true );

			System.out.println("Initializing cache connection pool..." + new Date().toString());
			cacheConnectionPool.initialize();

			sockPoolInitialized = true;
		}



		//		builder = new MemcachedClientBuilder( 
		//				AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
		//		builder.setConnectionPoolSize(CACHE_POOL_NUM_CONNECTIONS);

		initialized = true;
		try {
			cacheclient = new MemcachedClient(CACHE_POOL_NAME);
			cacheclient.setCompressEnable(false);
			//cacheclient_vector.add(cacheclient);
			//cacheclient.setMaxStaleness(max_staleness);
			//cacheclient.setMaxObjectSize(common.CacheUtilities.deserialize_buffer_size);
			crtcl.release();
		} catch (Exception e) {
			System.out.println("MemcacheClient init failed to release semaphore.");
			e.printStackTrace(System.out);
		}

		return true;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		try {
			//System.out.println("************number of threads="+NumThreads);
			cleanupAllConnections();
			//			if (warmup) decrementNumThreads();
			//			if (verbose) System.out.println("Cleanup (before warmup-chk):  NumThreads="+NumThreads.get());
			//			if(!warmup)
			{
				crtcl.acquire();

				//System.out.println("Database connections closed");


				decrementNumThreads();
				if (verbose) 
					System.out.println("Cleanup (after warmup-chk):  NumThreads="+NumThreads.get());
				if (NumThreads.get() > 0){
					crtcl.release();
					//cleanupAllConnections();
					//System.out.println("Active clients; one of them will clean up the cache manager.");
					return;
				}

				initialized = false;

				//				for(MemcachedClient client : cacheclient_vector)
				//				{
				//					client.shutdown();
				//				}

				//MemcachedClient cacheclient = new MemcachedClient(
				//		AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
				//cacheclient.printStats();
				//cacheclient.stats();
				//cacheclient.shutdown();


				if(cleanup_connectionpool) {
					cacheConnectionPool.shutDown();
					cacheConnectionPool = null;
					sockPoolInitialized = false;
					System.out.println("Connection pool shut down");					
				}


				if (ManageCache){
					System.out.println("Stopping Cache: "+this.getCacheStopCmd());

					if(USE_LISTENER_START_CACHE)
					{
						//common.CacheUtilities.runListener(cache_hostname, listener_port, "C:\\memcached\\memcached.exe -d stop");
						common.CacheUtilities.stopMemcached(cache_hostname, listener_port);
					}
					else
					{
						//MemcachedClient cache_conn = new MemcachedClient(COSARServer.cacheServerHostname, COSARServer.cacheServerPort);			
						//cache_conn.shutdownServer();

						//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
						this.st = new StartProcess(this.getCacheStopCmd(), "cache_output.txt");
						this.st.start();
					}
					System.out.print("Waiting for Cache to finish.");

					if( this.st != null )
						this.st.join();
					Thread.sleep(10000);
					System.out.println("..Done!");
				}

				crtcl.release();
			}
		} catch (InterruptedException IE) {
			System.out.println("Error in cleanup:  Semaphore interrupt." + IE);
			throw new DBException(IE);
		} catch (Exception e) {

			System.out.println("Error in closing the connection. " + e);
			throw new DBException(e);
		}
	}

	protected PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}

	@Override
	//Load phase query not cached
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
		return common.RdbmsUtilities.insertEntityBoosted(entitySet, entityPK, values, insertImage, conn, FSimagePath);
	}

	@Override
	public void createSchema(Properties props){
		common.RdbmsUtilities.createSchemaBoosted2R1T(props, conn);
	}

	@Override
	public void buildIndexes(Properties props){
		common.RdbmsUtilities.buildIndexes(props, conn);
	}

	public static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	public static void dropIndex(Statement st, String idxName) {
		try {
			st.executeUpdate("drop index " + idxName);
		} catch (SQLException e) {
		}
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
		}
	}

	public static void dropStoredProcedure(Statement st, String procName) {
		try {
			st.executeUpdate("drop procedure " + procName);
		} catch (SQLException e) {
		}
	}


	private String md5Java(byte[] hash){
		String digest = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			hash = md.digest(hash);

			//converting byte array to Hexadecimal String
			StringBuilder sb = new StringBuilder(2*hash.length);
			for(byte b : hash){
				sb.append(String.format("%02x", b&0xff));
			}

			digest = sb.toString();

		} catch (NoSuchAlgorithmException ex) {
			System.out.println(ex.getStackTrace());
		}
		
		return digest;
	}
}