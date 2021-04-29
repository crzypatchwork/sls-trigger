'use strict';

const axios = require('axios')
const MongoClient = require('mongodb').MongoClient
require('dotenv').config()

const url = process.env.MONGO_URI
const dev = 'https://api.better-call.dev/v1/contract/mainnet/KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton/tokens'
const client = new MongoClient(url)


const getFeed = async (arr, counter, objkts) => {

  // gets latest objkts

  let res = await axios.get("https://api.better-call.dev/v1/contract/mainnet/KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton/tokens?offset=" + counter).then(res => res.data)

  res = await res.map(async e => {

    // fails on unique keys

    try {
      console.log(e.token_id)
      await objkts.insertOne(e)
      return true
    } catch (err) {
      console.log('err', e.token_id)
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

const insertFeed = async () => {
  await client.connect()
  const database = client.db('OBJKTs-DB')
  const objkts = database.collection('metadata')
  //await objkts.createIndex( { 'token_id' : 1 }, { unique: true } )
  await getFeed([], 0, objkts)
}

//insertFeed()

module.exports.insert = async (event) => {
  await insertFeed()
  return {
    status : 200
  }
};
