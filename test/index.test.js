const { AthenaQuery, athenaQuery } = require('..');

/**
 * Athena Search Service
 */
class AthenaClient {
  /**
   * Add where
   * @param {Object} and where
   */
  andWhere(and, where = []) {
    const toString = value => {
      if (typeof value === 'number') {
        return value;
      }
      // escape injection
      return `'${value.replace(/'/, "''")}'`;
    };
    Object.entries(and).forEach(args => {
      const [key, value] = args;
      if (key === 'col-static') {
        where.push(...value);
        return;
      }
      if (Array.isArray(value)) {
        where.push(`${key} IN (${value.map(v => toString(v)).join(', ')})`);
        return;
      }
      if (typeof value === 'object') {
        Object.entries(value).forEach(([operator, attribute]) => {
          where.push(`${key} ${operator} ${toString(attribute)}`);
        });
        return;
      }
      where.push(`${key} = ${toString(value)}`);
    });
    return where;
  }

  /**
   * Get execution
   * @param {Object} execution execution
   */
  getExecution(execution) {
    return athenaQuery.getExecution(execution).then(status => {
      if (['FAILED', 'CANCELLED'].includes(status.State)) {
        return 'FAILED';
      }
      return status.State;
    });
  }

  setConf(conf) {
    athenaQuery.setConf(conf);
  }

  findData(ts) {
    const athena = new AthenaQuery({ ...athenaQuery.conf });

    const where = this.andWhere({
      ts: { '=': ts },
      message: { 'NOT LIKE': 'debug' },
      type: ['a', 'A'],
    });
    const sql = `SELECT
    *
    FROM sample_table
    WHERE '${where.join(' AND ')}'
    LIMIT 10`;

    return athena.execute(sql)
    .then(execution => {
      const { QueryExecutionId } = execution;
      return athenaQuery.waitForExecution({ QueryExecutionId }, { sql });
    })
    .then(execution => athenaQuery.getResult(execution))
    .then(result => result.rows);
  }
}

const athenaClient = new AthenaClient();
athenaClient.findData(new Date().toISOString().split('T')[0])
