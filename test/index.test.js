import { jest } from '@jest/globals';

jest.unstable_mockModule('@aws-sdk/client-athena', () => {
  const AthenaClient = jest.fn();
  AthenaClient.prototype.send = jest.fn();
  return {
    AthenaClient,
    StartQueryExecutionCommand: jest.fn(),
    GetQueryExecutionCommand: jest.fn(),
    GetQueryResultsCommand: jest.fn(),
  };
});

const {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} = await import('@aws-sdk/client-athena');

const { AthenaQuery, athenaQuery } = await import('../index.js');

// Capture the instance created by index.js immediately
const athenaInstance = AthenaClient.mock.instances[0];

describe('AthenaQuery', () => {
  let mockSend;

  beforeEach(() => {
    // Reset config
    athenaQuery.setConf({
      Database: 'default',
      MaxResults: 1000,
      OutputLocation: 's3://athena-query-results/athena-query-results/',
    });

    // Default to SUCCEEDED
    mockSend = jest.fn().mockResolvedValue({
      QueryExecution: { Status: { State: 'SUCCEEDED' } },
    });

    if (athenaInstance) {
      athenaInstance.send = mockSend;
    } else {
      // Fallback
      AthenaClient.prototype.send = mockSend;
    }
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should initialize with default config', () => {
      const query = new AthenaQuery();
      expect(query.conf).toEqual({
        Database: 'default',
        MaxResults: 1000,
        OutputLocation: 's3://athena-query-results/athena-query-results/',
      });
    });

    it('should initialize with custom config', () => {
      const conf = { Database: 'test-db' };
      const query = new AthenaQuery(conf);
      expect(query.conf.Database).toBe('test-db');
    });
  });

  describe('setConf', () => {
    it('should update configuration', () => {
      athenaQuery.setConf({ MaxResults: 500 });
      expect(athenaQuery.conf.MaxResults).toBe(500);
    });
  });

  describe('toValue', () => {
    it('should convert integer strings to numbers', () => {
      expect(athenaQuery.toValue('integer', '123')).toBe(123);
      expect(athenaQuery.toValue('bigint', '456')).toBe(456);
    });

    it('should return undefined for invalid numbers', () => {
      expect(athenaQuery.toValue('integer', 'abc')).toBeUndefined();
    });

    it('should parse array strings', () => {
      expect(athenaQuery.toValue('array', '[a, b]')).toEqual(['a', 'b']);
      expect(athenaQuery.toValue('array', '[null]')).toEqual([]);
    });

    it('should return value as is for other types', () => {
      expect(athenaQuery.toValue('string', 'test')).toBe('test');
    });
  });

  describe('execute', () => {
    it('should send StartQueryExecutionCommand with correct params', async () => {
      mockSend.mockResolvedValue({ QueryExecutionId: '123' });
      const sql = 'SELECT * FROM table';
      await athenaQuery.execute(sql);

      expect(mockSend).toHaveBeenCalledWith(expect.any(StartQueryExecutionCommand));
      expect(StartQueryExecutionCommand).toHaveBeenCalledWith({
        QueryString: sql,
        QueryExecutionContext: { Database: athenaQuery.conf.Database },
        ResultConfiguration: { OutputLocation: athenaQuery.conf.OutputLocation },
      });
    });
  });

  describe('getExecution', () => {
    it('should return execution status', async () => {
      mockSend.mockResolvedValue({
        QueryExecution: { Status: { State: 'SUCCEEDED' } },
      });
      const status = await athenaQuery.getExecution({ QueryExecutionId: '123' });
      expect(status).toEqual({ State: 'SUCCEEDED' });
      expect(GetQueryExecutionCommand).toHaveBeenCalledWith({ QueryExecutionId: '123' });
    });
  });

  describe('waitForExecution', () => {
    it('should resolve when status is SUCCEEDED', async () => {
      // mockSend already returns SUCCEEDED by default
      const result = await athenaQuery.waitForExecution({ QueryExecutionId: '123' });
      expect(result).toEqual({ QueryExecutionId: '123' });
    });

    it('should reject when status is FAILED', async () => {
      mockSend.mockResolvedValue({
        QueryExecution: { Status: { State: 'FAILED' } },
      });
      await expect(athenaQuery.waitForExecution({ QueryExecutionId: '123' }))
      .rejects.toThrow('Query FAILED');
    });
  });

  describe('getResult', () => {
    it('should fetch and parse results', async () => {
      const mockData = {
        ResultSet: {
          ResultSetMetadata: {
            ColumnInfo: [
              { Name: 'id', Type: 'integer' },
              { Name: 'name', Type: 'varchar' },
            ],
          },
          Rows: [
            { Data: [{ VarCharValue: 'id' }, { VarCharValue: 'name' }] }, // Header row
            { Data: [{ VarCharValue: '1' }, { VarCharValue: 'test' }] },
          ],
        },
      };
      mockSend.mockResolvedValue(mockData);

      const result = await athenaQuery.getResult({ QueryExecutionId: '123' });
      expect(result.rows).toEqual([{ id: 1, name: 'test' }]);
      expect(GetQueryResultsCommand).toHaveBeenCalledWith(expect.objectContaining({
        QueryExecutionId: '123',
        MaxResults: 1000,
      }));
    });

    it('should handle pagination', async () => {
      const mockPage1 = {
        ResultSet: {
          ResultSetMetadata: {
            ColumnInfo: [{ Name: 'id', Type: 'integer' }],
          },
          Rows: [
            { Data: [{ VarCharValue: 'id' }] }, // Header row
            { Data: [{ VarCharValue: '1' }] },
          ],
        },
        NextToken: 'token',
      };
      const mockPage2 = {
        ResultSet: {
          ResultSetMetadata: {
            ColumnInfo: [{ Name: 'id', Type: 'integer' }],
          },
          Rows: [{ Data: [{ VarCharValue: '2' }] }],
        },
      };

      mockSend
      .mockResolvedValueOnce(mockPage1)
      .mockResolvedValueOnce(mockPage2);

      const result = await athenaQuery.getResult({ QueryExecutionId: '123' });
      expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);
      expect(mockSend).toHaveBeenCalledTimes(2);
    });
  });
});
