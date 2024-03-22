const {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} = require('@aws-sdk/client-athena');

const athena = new AthenaClient();
const logger = console;
const Default = {
  Database: 'default',
  MaxResults: 1000,
  OutputLocation: 's3://athena-query-results/athena-query-results/',
};

/**
 * Athena Service Class
 */
class AthenaQuery {
  constructor(conf) {
    this.setConf(conf);
  }

  /**
   * Config
   * @param {Object} conf config
   */
  setConf(conf) {
    this.conf = { ...Default, ...conf };
  }

  /**
   * To value
   * @param {String} Type type
   * @param {any} value 値
   */
  toValue(Type, value) {
    if (['integer', 'bigint'].includes(Type)) {
      const number = parseInt(value, 10);
      if (Number.isNaN(number)) return undefined;
      return number;
    }
    if (Type === 'array') {
      return (value || '')
      .replace(/^\[|]$/g, '')
      .split(', ')
      .filter(v => v !== 'null');
    }
    return value;
  }

  /**
   * Get result
   * @param {Object} execution execution
   */
  getResult(execution, rows = []) {
    if (!execution.MaxResults) execution.MaxResults = this.conf.MaxResults;
    return athena.send(new GetQueryResultsCommand(execution))
    .then(async data => {
      const headers = data.ResultSet.ResultSetMetadata.ColumnInfo;
      rows.push(...data.ResultSet.Rows.map(v => {
        const row = {};
        v.Data.forEach((obj, index) => {
          const { Name, Type } = headers[index];
          const [value] = Object.values(obj);
          row[Name] = this.toValue(Type, value);
        });
        return row;
      }));
      if (data.NextToken) {
        await this.getResult({
          ...execution,
          NextToken: data.NextToken,
        }, rows);
      } else rows.shift();
      return { rows };
    });
  }

  /**
   * Wait for execution
   * @param {Object} options options
   */
  waitForExecution(options, extras) {
    const tryMilliseconds = 800;
    if (!options.prom) {
      const prom = { ms: new Date().getTime() };
      prom.pending = new Promise((...args) => { [prom.resolve, prom.reject] = args; });
      options.prom = prom;
    }

    const {
      QueryExecutionId,
      prom: { pending, resolve, reject },
    } = options;

    this.getExecution({ QueryExecutionId }).then(status => {
      if (status.State === 'SUCCEEDED') {
        logger.debug({
          benchmark: new Date().getTime() - options.prom.ms,
          QueryExecutionId,
        });
        resolve({ QueryExecutionId });
        return;
      }
      if (['FAILED', 'CANCELLED'].includes(status.State)) {
        reject(new Error(`Query ${status.State}`));
        return;
      }
      setTimeout((...args) => this.waitForExecution(...args), tryMilliseconds, options, extras);
    })
    .catch(e => reject(e));

    return pending;
  }

  /**
   * Get execution
   * @param {Object} execution execution
   */
  getExecution(execution) {
    return athena.send(new GetQueryExecutionCommand(execution))
    .then(data => data.QueryExecution.Status);
  }

  /**
   * Execute query
   * @param {String} sql query
   */
  execute(sql) {
    const params = {
      QueryString: sql,
      QueryExecutionContext: { Database: this.conf.Database },
      ResultConfiguration: { OutputLocation: this.conf.OutputLocation },
    };
    logger.debug(`[AthenaQuery] ${sql.replace(/\n|  /g, ' ')}`);
    return athena.send(new StartQueryExecutionCommand(params));
  }
}

module.exports = {
  AthenaQuery,
  athenaQuery: new AthenaQuery(),
};
