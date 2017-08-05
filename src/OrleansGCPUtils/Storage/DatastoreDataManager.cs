using Google.Api.Gax.Grpc;
using Google.Cloud.Datastore.V1;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Orleans.Providers.GCP;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansGCPUtils.Storage
{
    public class DatastoreDataManager
    {
        private DatastoreDb _db;
        private DatastoreClient _client;
        private readonly Logger _logger;
        private readonly string _projectId;
        private ServiceEndpoint _customEndpoint;

        public DatastoreDataManager(Logger baseLogger, string projectId, string customEndpoint = "")
        {
            if (string.IsNullOrWhiteSpace(projectId)) throw new ArgumentNullException(nameof(projectId));

            _logger = baseLogger?.GetSubLogger(GetType().Name);
            _projectId = projectId;

            if (!string.IsNullOrWhiteSpace(customEndpoint))
            {
                var hostPort = customEndpoint.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (hostPort.Length != 2) throw new ArgumentException(nameof(customEndpoint));

                var host = hostPort[0];
                int port;
                if (!int.TryParse(hostPort[1], out port)) throw new ArgumentException(nameof(customEndpoint));

                _customEndpoint = new ServiceEndpoint(host, port);
            }
        }

        public async Task Initialize()
        {
            try
            {
                _client = await DatastoreClient.CreateAsync(_customEndpoint);
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, "", "Initialize", GoogleErrorCode.Initializing);
            }

            try
            {
                _db = DatastoreDb.Create(_projectId, client: _client);
            }
            catch (RpcException e)
            {
                if (e.Status.StatusCode != StatusCode.AlreadyExists)
                    ReportErrorAndRethrow(e, "", "CreateTopicAsync", GoogleErrorCode.Initializing);
                throw;
            }
        }

        public async Task CreateEntityAsync(string kind, string key, MapField<string, Value> fields)
        {
            const string operation = "CreateEntry";

            if (_logger.IsVerbose2) _logger.Verbose2("Creating {0} entity: {1}", kind, key);

            try
            {
                Entity entity = GetEntity(kind, key, fields);

                using (var trx = await _db.BeginTransactionAsync())
                {
                    trx.Insert(entity);
                    await trx.CommitAsync();
                }
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, kind, operation, GoogleErrorCode.WriteDataStore);
            }
        }

        public async Task UpsertEntityAsync(string kind, string key, MapField<string, Value> fields)
        {
            const string operation = "UpsertEntry";

            if (_logger.IsVerbose2) _logger.Verbose2("Upserting {0} entity: {1}", kind, key);

            try
            {
                Entity entity = GetEntity(kind, key, fields);

                using (var trx = await _db.BeginTransactionAsync())
                {
                    trx.Upsert(entity);
                    await trx.CommitAsync();
                }
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, kind, operation, GoogleErrorCode.WriteDataStore);
            }
        }

        public async Task DeleteEntityAsync(string kind, string key)
        {
            const string operation = "DeleteEntry";

            if (_logger.IsVerbose2) _logger.Verbose2("Deleting {0} entity: {1}", kind, key);

            try
            {
                if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
                if (string.IsNullOrWhiteSpace(kind)) throw new ArgumentNullException(nameof(kind));

                Entity entity = await _db.LookupAsync(_db.CreateKeyFactory(kind).CreateKey(key));

                using (var trx = await _db.BeginTransactionAsync())
                {
                    trx.Delete(entity);
                    await trx.CommitAsync();
                }
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, kind, operation, GoogleErrorCode.WriteDataStore);
            }
        }

        /// <summary>
        /// Read a single entry from GCP Datastore
        /// </summary>
        /// <typeparam name="TResult">The result type</typeparam>
        /// <param name="kind">Entity kind</param>
        /// <param name="key">Entity key</param>
        /// <param name="resolver">Function that will be called to translate the returned fields into a concrete type. This Function is only called if the result is != null</param>
        /// <returns>The object translated by the resolver function<</returns>
        public async Task<TResult> ReadSingleEntryAsync<TResult>(string kind, string key, Func<MapField<string, Value>, TResult> resolver) where TResult : class
        {
            const string operation = "ReadSingleEntry";

            try
            {
                var query = new Query(kind) { Filter = Filter.Equal("__key__", _db.CreateKeyFactory(kind).CreateKey(key)) };
                var result = await _db.RunQueryAsync(query);
                if (result.Entities != null && result.Entities.Count > 0)
                {
                    return resolver(result.Entities[0].Properties);
                }
                else
                {
                    return null;
                }
            }
            catch (Exception e)
            {
                ReportErrorAndRethrow(e, kind, operation, GoogleErrorCode.ReadEntity);
                throw;
            }
        }

        

        private Entity GetEntity(string kind, string key, MapField<string, Value> fields)
        {
            if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrWhiteSpace(kind)) throw new ArgumentNullException(nameof(kind));
            if (fields == null) throw new ArgumentNullException(nameof(fields));

            var entity = new Entity()
            {
                Key = _db.CreateKeyFactory(kind).CreateKey(key)
            };

            foreach (var map in fields)
                entity[map.Key] = map.Value;
            return entity;
        }

        private void ReportErrorAndRethrow(Exception exc, string kind, string operation, GoogleErrorCode errorCode)
        {
            var errMsg = String.Format(
                "Error doing {0} for Google Project {1} at DataStore EntityKind {2} " + Environment.NewLine
                + "Exception = {3}", operation, _projectId, kind, exc);
            _logger.Error((int)errorCode, errMsg, exc);
            throw new AggregateException(errMsg, exc);
        }
    }
}
