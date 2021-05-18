'use strict'

const axios = require('axios')
const _ = require('lodash')
const MongoClient = require('mongodb').MongoClient
require('dotenv').config()

const url = process.env.MONGO_URI
const dev = 'https://api.better-call.dev/v1/contract/mainnet/KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton/tokens'
const client = new MongoClient(url)

// method to populate the DB with all owners via upsert. Takes around 60 mins (500k records).
const getOwners = async(arr,counter,owners) => {
  /*
  https://api.tzkt.io/v1/bigmaps/522/keys?sort.desc=id&select=key,value&offset=0&limit=10
  limit can be up to 1000
  [{"key":{"nat":"68596","address":"tz1d7i7nUREze4LCpfonUeaJgdfhcWnoy7p9"},"value":"1"},..]
  
  owners collection: {"token_id": 999999, "owner_id": "tz123", "balance": 0 }
  unique index on token_id, owner_id. Index on owner_id for search.
  */
  let res = await axios.get("https://api.tzkt.io/v1/bigmaps/511/keys?sort.desc=id&select=key,value&limit=50&offset=" + counter)
    .then(res => res.data)
  res = await res.map(async e => {

  try {
    const query = {"token_id": parseInt(e.key.nat), "owner_id": e.key.address}
    const update = { "$set": {"token_id":parseInt(e.key.nat), "owner_id": e.key.address, "balance": parseInt(e.value)} }
    const options = { upsert: true };
    console.log(e.key)
    let r = await owners.updateOne(query, update, options)
    if (r.modifiedCount === 1 || r.upsertedId !== null ) {
      return true //updated or inserted something new
    } else {
      return false //change from false to true to interate thru all results.
    }
  } catch (err) {
    console.log('err', e.key, err)
    return false
  }
})

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.every( e => e === false)) { //looks at all results to see if there was a change
      await getOwners(arr, counter + 50, owners) //fetch more records if some results were updated
    }
  })
  console.log('end')
  return [arr, ...res]

}

////////////////////////
// method to populate the DB with objkt curation hDAO balances via upsert. 
// Full refresh takes around 10 mins (70k records).

const getTokenCurationBalance = async(arr,counter,objkts) => {
  /*
  https://api.tzkt.io/v1/bigmaps/519/keys?sort.desc=id&select=key,value&offset=0&limit=10
  limit can be up to 1000
  [{"key":"69976","value":{"issuer":"tz1ZM3gyiFnaU9itgTshE7Z2jgG3bTk4TF2z","hDAO_balance":"4471"}},..]
  
  `token_curations` collection: {"token_id": 999999, "hdao_balance": 0 }
  unique index on token_id.
  *** API has `hDAO_balance` in objkt
  Given the 1:1 relationship with objkt, this should be an object on `metadata` collection.
  Iterate through bigmap. stop when no more updates after a `counter` updates no more records
  Update only, if token_id missing, then will pick up in next call.
  */

  let res = await axios.get("https://api.tzkt.io/v1/bigmaps/519/keys?sort.desc=id&select=key,value&limit=50&offset=" + counter)
    .then(res => res.data)
  res = await res.map(async e => {

  try {
    const query = { "token_id": parseInt(e.key), hDAO_balance: {"$ne": parseInt(e.value.hDAO_balance) } }
    const update = { "$set": {"hDAO_balance": parseInt(e.value.hDAO_balance)} }
    console.log(e.key, e.value)
    let r = await objkts.findOneAndUpdate(query, update)
    console.log(r.lastErrorObject.updatedExisting)
    if (r.lastErrorObject.updatedExisting === true ) {
      return true //updated or inserted something new
    } else {
      return false //change from false to true to interate thru all results.
    }
  } catch (err) {
    console.log('err', e.key, err)
    return false
  }
})

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.every( e => e === false)) { //looks at all results to see if there was a change
      await getTokenCurationBalance(arr, counter + 50, objkts) //fetch more records if some results were updated
    }
  })
  console.log('end')
  return [arr, ...res]

}

////////////////////////
// method to populate the DB with swaps via upsert. 
// Full refresh takes around 10 mins (150k records).

const getSwaps = async(arr,counter,swaps) => {
  /*
  https://api.tzkt.io/v1/bigmaps/523/keys?sort.desc=id&offset=0&limit=100
  limit can be up to 1000
  [{"id":1210695,"active":true,"hash":"expruPNtEb5v3PLGHEFrnQTNAL7TZX38TQkvWcj3KAYNZ8PMp3yxwQ",
    "key":"150293","value":{"issuer":"tz1ViMzA17dRAZpCopuckBHdphMua26YDVgN","objkt_id":"46019",
    "objkt_amount":"1","xtz_per_objkt":"4300000"},"firstLevel":1473980,"lastLevel":1473980,
    "updates":1}, ...]
  
  `swaps` collection: {"token_id": 999999, "swap_id": 12345, "objkt_amount": 1, "xtz_per_objkt": 100000,
      "issuer": "tz123", "active": true}
  unique index on swap_id. natural key on token_id, issuer. facts: active (status), price, amt.
  
  Iterate through bigmap. stop when no more updates after a `counter` updates no more records
  Update only, if token_id missing, then will pick up in next call.
  */

  let res = await axios.get("https://api.tzkt.io/v1/bigmaps/523/keys?sort.desc=id&limit=100&offset=" + counter)
    .then(res => res.data)
  res = await res.map(async e => {

  try {
    const query = { 
      swap_id: parseInt(e.key), 
        "$or": [
          {status: {"$ne": e.value.active}},
          {objkt_amount: {"$ne": parseInt(e.value.objkt_amount)}},
          {xtz_per_objkt: {"$ne": parseInt(e.value.xtz_per_objkt)}}
        ]
    }
    const update = {
      "$set": {
        token_id: parseInt(e.value.objkt_id), 
        issuer: e.value.issuer,
        status: e.active,
        objkt_amount: parseInt(e.value.objkt_amount),
        xtz_per_objkt: parseInt(e.value.xtz_per_objkt)
      }
    }
    const options = { upsert: true };
    console.log(e.id, "swap_id:", e.key) //, e.value, e.active)
    let r = await swaps.updateOne(query, update, options)
    if (r.modifiedCount === 1 || r.upsertedId !== null ) {
      //console.log("t", r.modifiedCount, r.upsertedId)
      return true //updated or inserted something new
    } else {
      //console.log("f", r.modifiedCount, r.upsertedId)
      return false //change from false to true to interate thru all results.
    }
  } catch (err) {
    console.log('err', e.key, err)
    return false
  }
})

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.every( e => e === false)) { //looks at all results to see if there was a change
      await getSwaps(arr, counter + 100, swaps) //fetch more records if some results were updated
    }
  })
  console.log('end')
  return [arr, ...res]

}


////////////////////////
// method to populate the DB with user hDAO via upsert. 
// Full refresh takes around 2 mins (3k records).

const getHDAOBalances = async(arr,counter,hDAOBalances) => {
  /*
  https://api.tzkt.io/v1/bigmaps/515/keys?sort.desc=id&select=key,value&offset=0&limit=100
  limit can be up to 1000
  [{"key":{"nat":"0","address":"tz1gk59qsxEYJyq1owS6NqV61hA2nD8nihyH"},"value":"99651"}, ...]
  
  `hdao_balances` collection: {"wallet": "tz123", balance: 12345}
  unique index on wallet. facts: balance. Balances are set to 0 rather than active=false.
  
  Iterate through bigmap. stop when no more updates after a `counter` updates no more records
  Update only, if token_id missing, then will pick up in next call.
  */

  let res = await axios.get("https://api.tzkt.io/v1/bigmaps/515/keys?sort.desc=id&select=key,value&limit=50&offset=" + counter)
    .then(res => res.data)
  res = await res.map(async e => {

  try {
    const query = { 
      wallet: e.key.address, balance: {"$ne": parseInt(e.value)} 
    }
    const update = {"$set": {value: parseInt(e.value)}}
    const options = { upsert: true }
    console.log("wallet:", e.key.address)
    let r = await hDAOBalances.updateOne(query, update, options)
    if (r.modifiedCount === 1 || r.upsertedId !== null ) {
      //console.log("t", r.modifiedCount, r.upsertedId)
      return true //updated or inserted something new
    } else {
      //console.log("f", r.modifiedCount, r.upsertedId)
      return false //change from false to true to interate thru all results.
    }
  } catch (err) {
    console.log('err', e.key, err)
    return false
  }
})

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.every( e => e === false)) { //looks at all results to see if there was a change
      await getHDAOBalances(arr, counter + 50, hDAOBalances) //fetch more records if some results were updated
    }
  })
  console.log('end')
  return [arr, ...res]

}


//////////////
const getRoyalties = async(arr,counter,objkts) => {

  // https://api.tzkt.io/v1/bigmaps/522/keys?sort.desc=id&select=key,value&offset=0&limit=10
  // limit can be up to 1000
  // default is in ascending order
  // {"key":"152","value":{"issuer":"tz1UBZUkXpKGhYsP5KtzDNqLLchwF4uHrGjw","royalties":"100"}}
  let res = await axios.get("https://api.tzkt.io/v1/bigmaps/522/keys?sort.desc=id&select=key,value&limit=20&offset=" + counter)
  .then(res => res.data)
  res = await res.map(async e => {

    try {
      const query = { "token_id": parseInt(e.key), royalties: {"$ne": parseInt(e.value.royalties)/1000 } }
      const update = { "$set": {"royalties": parseInt(e.value.royalties)/1000 } }
      console.log(e.key, e.value)
      let r = await objkts.findOneAndUpdate(query, update)
      console.log(r.lastErrorObject.updatedExisting)
      if (r.lastErrorObject.updatedExisting === true ) {
        return true //updated or inserted something new
      } else {
        return false //change from false to true to interate thru all results.
      }
    } catch (err) {
      console.log('err', e.key, err)
      return false
    }
  })

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.every( e => e === false)) {
      await getRoyalties(arr, counter + 20, objkts)
    }
  })
  console.log('end')
  return [arr, ...res]

}

const getFeed = async (arr, counter, objkts) => {

  // gets latest objkts

  let res = await axios
    .get("https://api.better-call.dev/v1/contract/mainnet/KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton/tokens?offset=" + counter)
    .then(res => res.data)

  res = await res.map(async e => {

    // fails on unique keys

    try {
      console.log(e.token_id)
      await objkts.insertOne(e)
      return true
    } catch (err) {
      //console.log('err', e.token_id, err)
      return false
    }

  })

  var promise = Promise.all(res.map(e => e))

  promise.then(async (results) => {
    if (!results.includes(false)) {
      await getFeed(arr, counter + 10, objkts)
    }
  })
  console.log('end')
  return [arr, ...res]

}

///////
// subjkts methods

const registries = 3536
const subjkts = 3537
const subjkts_metadata = 3538

const kt = 'KT1P69B8exDGuqNysBweuZJSqAmaD4dU3gtU'
//https://api.better-call.dev/v1/contract/mainnet/KT1P69B8exDGuqNysBweuZJSqAmaD4dU3gtU/storage

const getRegistries = async () => {

	let res = await axios.get('https://api.better-call.dev/v1/bigmap/mainnet/3536/keys').then(res => res.data)

	return res.map(e => {
		return { tz : e.data.key.value, subjkt : e.data.value !== null ? e.data.value.value : null }
	})

}

const getSubjktsMetadata = async () => {

	let res = await axios.get('https://api.better-call.dev/v1/bigmap/mainnet/3538/keys').then(res => res.data)

	res = res.map(e => { 
		return { subjkt : e.data.key.value, ipfs : e.data.value !== null ? e.data.value.value : null }
	})

	return res.map(async e => {
		if (e.ipfs !== null) {
			e.metadata = await axios.get(`https://ipfs.io/ipfs/${(e.ipfs).split('//')[1]}`).then(res => res.data)
			return e
		} else {
			return e
		}
	})
	
}

const merge = async (subjkt) => {

	let promise = Promise.all((await getSubjktsMetadata()).map(e => e))
  	promise.then(async (metadata) => {
      //console.log(metadata)
    	let result = _.merge(_.keyBy(await getRegistries(), 'subjkt'), _.keyBy(metadata, 'subjkt'))
      await subjkt.insertMany(_.values(result))
  	})
    //console.log(await getRegistries())
}

const insertSubjkts = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const subjkt = database.collection('subjkt')
  await merge(subjkt)
  // for string?
  // await subjkt.createIndex( { 'subjkt' : 1 }, { unique : true } ) 

}

const insertFeed = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const objkts = database.collection('metadata')
  //await objkts.createIndex( { 'token_id' : 1 }, { unique: true } )
  await getFeed([], 0, objkts)
}

const insertRoyalties = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const objkts = database.collection('metadata')
  await getRoyalties([], 0, objkts)
}

const insertTokenOwners = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const owners = database.collection('owners')
  //await owners.createIndex( { 'token_id' : 1, 'owner_id' : 1 }, { unique: true } )
  //await owners.createIndex( { 'owner_id' : 1 } )
  await getOwners([], 0, owners)
}

const insertTokenCurationBalance = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const objkts = database.collection('metadata')
  await getTokenCurationBalance([], 0, objkts)
}

const insertSwaps = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const swaps = database.collection('swaps')
  await getSwaps([], 0, swaps)
}

const insertHDAOBalances = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const hDAOBalances = database.collection('hdao_balances')
  await getHDAOBalances([], 0, hDAOBalances)
}

//insertFeed()
//insertRoyalties()
//insertTokenOwners()
//insertTokenCurationBalance()
//insertSwaps()
//insertHDAOBalances()
insertSubjkts()

/* module.exports.insert = async (event) => {
  await insertFeed()
  await insertRoyalties()
  await insertTokenOwners()
  await insertTokenCurationBalance()
  await insertSwaps()
  await insertHDAOBalances()
  //await insertSubjkts()
  return {
    status : 200
  }
};
 */