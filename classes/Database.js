const { MongoClient, ServerApiVersion } = require("mongodb");
const AoiError = require("aoi.js/src/classes/AoiError");

const EventEmitter = require("events");
class Database extends EventEmitter {
  constructor(client, options) {
    super();

    this.client = client;
    this.options = options;
    this.debug = this.options.debug ?? false;

    this.connect();
  }

  async connect() {
    try {
      this.client.db = new MongoClient(this.options.url, {
        serverApi: {
          version: ServerApiVersion.v1,
          strict: true,
          deprecationErrors: false
        },
        connectTimeoutMS: 15000
      });

      if (!this.options.tables || this.options.tables.length === 0) throw new TypeError("Missing variable tables, please provide at least one table.");
      if (this.options.tables.includes("__aoijs_vars__")) throw new TypeError("'__aoijs_vars__' is reserved as a table name.");
      this.client.db.tables = [...this.options.tables, "__aoijs_vars__"];

      this.client.db.get = this.get.bind(this);
      this.client.db.set = this.set.bind(this);
      this.client.db.drop = this.drop.bind(this);
      this.client.db.delete = this.delete.bind(this);
      this.client.db.deleteMany = this.deleteMany.bind(this);
      this.client.db.findOne = this.findOne.bind(this);
      this.client.db.findMany = this.findMany.bind(this);
      this.client.db.all = this.all.bind(this);
      this.client.db.db.transfer = this.transfer.bind(this);
      this.client.db.db.avgPing = this.ping.bind(this);
      this.client.db.db.readyAt = Date.now();

      await this.client.db.connect();

      if (this.options.logging !== false) {
        const latency = await this.client.db.db.avgPing();
        const { version } = require("../package.json");
        if (latency !== "-1") {
          AoiError.createConsoleMessage(
            [
              { text: `Successfully connected to MongoDB`, textColor: "white" },
              { text: `Cluster Latency: ${latency}ms`, textColor: "white" },
              { text: `Installed on v${version}`, textColor: "green" }
            ],
            "white",
            { text: " aoi.js-mongo  ", textColor: "cyan" }
          );
        }
      }
      
      for (const table of this.client.db.tables) {
        await this.client.cacheManager.createCache("Group", `c_${table}`);
      }
      console.log('Create all cache tables');
      this.emit("ready", { client: this.client });
    } catch (err) {
      AoiError.createConsoleMessage(
        [
          { text: `Failed to connect to MongoDB`, textColor: "red" },
          { text: err.message, textColor: "white" }
        ],
        "white",
        { text: " aoi.mongo  ", textColor: "cyan" }
      );
      process.exit(0);
    }

    if (this.options?.convertOldData?.enabled === true) {
      this.client.once("ready", () => {
        require("./backup")(this.client, this.options);
      });
    }
  }

  async load(table) {
    const db = this.client.db.db(table);
    const collections = await db.listCollections().toArray();

    for (const collection of collections) {
      const col = db.collection(collection.name);
      const data = await col.find({}).toArray();

      const cacheName = `c_${table}`;

      for (const item of data) {
        await this.client.cacheManager.caches["Group"][cacheName].set(item.key, item.value);
      }
    }
  }

  async ping() {
    let start = Date.now();
    const res = await this.client.db.db("admin").command({ ping: 1 });
    if (!res.ok) return -1;
    return Date.now() - start;
  }

  async get(table, key, id = undefined) {
    let cacheKey = key;
    if (id) cacheKey = `${key}_${id}`;
    const cacheName = `c_${table}`;
    const aoijs_vars = ["cooldown", "setTimeout", "ticketChannel"];
    const cache = this.client.cacheManager.caches["Group"][cacheName];
  
    if (this.debug) {
      console.log(`[received] get(${table}, ${key}, ${id})`);
    }
  
    let cachedValue = await cache.get(cacheKey);
    let data;
  
    if (cachedValue !== undefined) {
      data = { key: cacheKey, value: cachedValue };
      if (this.debug) {
        console.log(`[returning] get(${table}, ${key}, ${id}) -> cache value: ${typeof data === "object" ? JSON.stringify(data) : data}`);
      }
    } else {
      if (aoijs_vars.includes(key)) {
        data = await this.client.db.db(table).collection(key).findOne({ key: cacheKey });
        if (data) {
          await cache.set(cacheKey, data.value);
          data = { key: cacheKey, value: data.value };
        } else {
          data = { key: cacheKey, value: null };
        }
      } else {
        if (!this.client.variableManager.has(key, table)) return;
        
        data = await this.client.db.db(table).collection(key).findOne({ key: cacheKey });
        if (data) {
          await cache.set(cacheKey, data.value);
          data = { key: cacheKey, value: data.value };
        } else {
          const __var = this.client.variableManager.get(key, table)?.default;
          data = { key: cacheKey, value: __var };
          await cache.set(cacheKey, __var);
        }
      }
  
      if (this.debug) {
        console.log(`[returning] get(${table}, ${key}) -> fetched value: ${typeof data === "object" ? JSON.stringify(data) : data}`);
      }
    }
  
    return data;
  }  


  async set(table, key, id, value) {
    let cacheKey = key;
    if (id) cacheKey = `${key}_${id}`;

    if (this.debug) {
      console.log(`[received] set(${table}, ${key}, ${id}, ${typeof value === "object" ? JSON.stringify(value) : value})`);
    }

    await this.client.cacheManager.caches["Group"][cacheName][
      this.client.cacheManager.caches["Group"][cacheName].set ? "set" : "add"
    ](cacheKey, value);

    const col = this.client.db.db(table).collection(key);
    await col.updateOne({ key: cacheKey }, { $set: { value: value } }, { upsert: true });

    if (this.debug) {
      console.log(`[returning] set(${table}, ${key}, ${value}) -> ${typeof value === "object" ? JSON.stringify(value) : value}`);
    }
  }

  async drop(table, variable) {
    const cacheName = `c_${table}`;
    const cache = this.client.cacheManager.caches["Group"][cacheName];

    if (this.debug) {
      console.log(`[received] drop(${table}, ${variable})`);
    }
    if (variable) {
      await this.client.db.db(table).collection(variable).drop();
    } else {
      await this.client.db.db(table).dropDatabase();
    }

    for (const key of cache.keys()) {
      cache.delete(key);
    }

    if (this.debug) {
      console.log(`[returning] drop(${table}, ${variable}) -> dropped ${table}`);
    }
  }

  async deleteMany(table, query) {
    const cacheName = `c_${table}`;

    if (this.debug == true) {
      console.debug(`[received] deleteMany(${table}, ${query})`);
    }

    const cache = this.client.cacheManager.caches["Group"][cacheName];

    const keysToDelete = [];

    for (const [key] of cache.cache) {
      const cacheKey = key.split("_")[0];
      if (this.client.variableManager.has(cacheKey, table)) {
        const __var = this.client.variableManager.get(cacheKey, table)?.default;
        if (__var === undefined) continue;

        if (query && query(cacheKey)) {
          keysToDelete.push(key);
        }
      }
    }

    if (this.debug == true) {
      const data = await col.find(query).toArray();
      console.debug(`[returning] deleteMany(${table}, ${query}) -> ${data}`);
    }
    for (const key of keysToDelete) {
      cache.delete(key);
    }

    const db = this.client.db.db(table);
    const collections = await db.listCollections().toArray();

    for (let collection of collections) {
      const col = db.collection(collection.name);
      await col.deleteMany(query);
    }

    if (this.debug == true) {
      console.debug(`[returning] deleteMany(${table}, ${query}) -> deleted`);
    }
  }

  async delete(table, key, id) {
    let dbkey = key;
    if (id) dbkey = `${key}_${id}`;
    const cacheName = `c_${table}`;

    if (this.debug) {
      console.log(`[received] delete(${table}, ${key}, ${id})`);
    }
    const db = this.client.db.db(table);
    const collections = await db.listCollections().toArray();

    for (let collection of collections) {
      const col = db.collection(collection.name);
      const doc = await col.findOne({ key: dbkey });

      if (!doc) continue;

        if (this.debug) {
        console.log(`[returning] delete(${table}, ${key}) -> ${doc.value}`);
        }

        await col.deleteOne({ key: dbkey });
        this.client.cacheManager.caches["Group"][cacheName].delete(dbkey);

        break;
    }
      if (this.debug == true) {
        console.debug(`[returned] delete(${table}, ${key}) -> deleted`);
      }
  }

  async findOne(table, query) {
    const col = this.client.db.db(table).collection(query);
    return await col.findOne({}, { value: 1, _id: 0 });
  }

  async findMany(table, query, limit) {
    const db = this.client.db.db(table);
    const collections = await db.listCollections().toArray();
    let results = [];

    for (let collection of collections) {
      const col = db.collection(collection.name);
      let data;

      if (typeof query === "function") {
        data = await col.find({}).toArray();
        data = data.filter(query);
      } else {
        data = await col.find(query).toArray();
      }

      if (limit) {
        data = data.slice(0, limit);
      }

      results.push(...data);
    }

    return results;
  }

  async all(table, filter, list = 100, sort = "asc") {
    const db = this.client.db.db(table);
    const collections = await db.listCollections().toArray();
    let results = [];
    if (this.debug == true) {
      console.log(`[received] all(${table}, ${filter}, ${list}, ${sort})`);
    }
    for (let collection of collections) {
      const col = db.collection(collection.name);
      let data = await col.find({}).toArray();
      data = data.filter(filter);
      results.push(...data);
    }

    if (sort === "asc") {
      results.sort((a, b) => a.value - b.value);
    } else if (sort === "desc") {
      results.sort((a, b) => b.value - a.value);
    }
    if (this.debug == true) {
      console.log(`[returning] all(${table}, ${filter}, ${list}, ${sort}) -> ${JSON.stringify(results)} items`);
    }
    return results.slice(0, list);
  }

  async transfer() {
    require("./backup")(this.client, this.options);
  }
}

module.exports = { Database };
