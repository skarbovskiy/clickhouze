const ClickHouse = require('./client')

function insert (ch, table, obj) {
	return new Promise((resolve, reject) => {
		const keys = []
		const values = []

		for (const key in obj) {
			if (obj[key] == null) {
				continue;
			}
			let value = escape(obj[key]);
			keys.push(key)
			values.push(value)
		}

		const query = `insert into ${table}(${keys.join(',')}) values (${values.join(',')})`
		return ch.query(query, (err, res) => {
			if (err) {
				reject(err)
			} else {
				resolve()
			}
		})
	})
}

function batchInsert (ch, table, rows) {
	if (!Array.isArray(rows) || !rows.length) {
		throw new Error('Batch insert rows are missing')
	}
	return new Promise((resolve, reject) => {
		const keys = Object.keys(rows[0])
		const values = []
		rows.forEach(row => {
			values.push(Object.values(row).map(escape))
		})
		const query = `insert into ${table} (${keys.join(',')}) values ${values.map(row => `(${row.join(',')})`)}`
		return ch.query(query, (err, res) => {
			if (err) {
				reject(err)
			} else {
				resolve()
			}
		})
	})
}

function query (ch, sql) {
	sql += ' format JSON'
	return new Promise((resolve, reject) => {
		ch.query(sql, (err, res) => {
			if (err) {
				reject(err)
			} else {
				if (!res.body) {
					resolve();
				}
				try {
					resolve(JSON.parse(res.body).data)
				} catch (e) {
					reject(new Error(`Unable to parse clickhouse response body: ${res.body}`))
				}
			}
		})
	})
}

function queryRaw (ch, sql) {
	return new Promise((resolve, reject) => {
		ch.query(sql, (err, res) => {
			if (err) {
				reject(err)
			} else {
				resolve(res.body)
			}
		})
	})
}

function querySingle (ch, sql) {
	return query(ch, sql).then(res => {
		if (res && res.length) {
			return res[0]
		}
		return null
	})
}

function escape (val) {
	if (val === null) {
		return "'" + '\\N' + "'";
	}
	if (Array.isArray(val)) {
		return '[' + val.map(i => typeof 'string' ?`'${i}'` : i).join(', ') + ']';
	}
	if (typeof val === 'string') {
		return "'" + val.replace(/\\/g, '\\\\').replace(/'/g, '\\\'') + "'"
	}
	return val
}

function queryJSONStream (ch, sql) {
	sql += ' format JSON'
	return ch.stream(sql)
}

module.exports = function buildClient (config) {
	const ch = new ClickHouse(config)

	return {
		client: ch,
		insert: insert.bind(null, ch),
		batchInsert: batchInsert.bind(null, ch),
		query: query.bind(null, ch),
		queryRaw: queryRaw.bind(null, ch),
		querySingle: querySingle.bind(null, ch),
		queryJSONStream: queryJSONStream.bind(null, ch),
	}
}
