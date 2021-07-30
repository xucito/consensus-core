using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.StateObjects;
using ConsensusCore.TestNode.Models;
using LiteDB;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Db
{
    public class EntitiesRepository
    {
        private string _databaseLocation;
        LiteDatabase _db;
        ConcurrentDictionary<string, string> keyDict = new ConcurrentDictionary<string, string>();
        private object _writeLock = new object();
        BsonMapper _mapper;

        public EntitiesRepository(LiteDatabase db)
        {
            _db = db;
            _db.Checkpoint();
            _mapper = new BsonMapper();
        }

        public void Setup()
        {
            var mapper = BsonMapper.Global;
            /*mapper.Entity<NodeStorage<CindiClusterState>>()
                .Ignore(x => x.CommandsQueue);*/
            BsonMapper.Global.RegisterType<TestState>(a =>
            {
                return JsonConvert.SerializeObject(a);
            }, b =>
            {
                return JsonConvert.DeserializeObject<TestState>(b.AsString);
            });

            var swo = _db.GetCollection<ShardWriteOperation>(NormalizeCollectionString(typeof(ShardWriteOperation)));
            swo.EnsureIndex(o => o.Pos);
            swo.EnsureIndex(o => o.Id);
            swo.EnsureIndex(o => o.Operation);
        }

        public string NormalizeCollectionString(Type type)
        {
            if (!keyDict.ContainsKey(type.Name))
            {
                keyDict.TryAdd(type.Name, Regex.Replace(type.Name, @"[^0-9a-zA-Z:,]+", "_"));
            }

            return keyDict[type.Name];
        }

        public long Count<T>(Expression<Func<T, bool>> expression = null)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
            return collection.Count(expression);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(Expression<Func<T, bool>> expression = null, List<Expression<Func<T, object>>> exclusions = null, string sort = null, int size = 10000, int page = 0)
        {
            string sortField = null;
            string order = null;
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
            string field = null;
            if (sort != null)
            {
                var split = sort.Split(",")[0];
                field = split.Split(":")[0];
                order = split.Split(":")[1].ToLower();
            }

            if (expression == null)
            {
                expression = _ => true;
            }

            IEnumerable<T> result;
            if (field != null)
            {
                var bsonExpression = _mapper.GetExpression(expression);
                ILiteQueryableResult<T> queryable = null;
                if (order == "1")
                {
                    queryable = collection.Query().Where(bsonExpression).OrderBy(BsonExpression.Create(field)).Skip(page * size).Limit(size);
                }
                else
                {
                    queryable = collection.Query().Where(bsonExpression).OrderByDescending(BsonExpression.Create(field)).Skip(page * size).Limit(size);
                }
                result = queryable.ToEnumerable();
            }
            else
                result = collection.Find(expression).Skip(page * size).Take(size);
            return result;
        }

        public async Task<bool> Delete<T>(Expression<Func<T, bool>> expression)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T))); ;
            collection.DeleteMany(expression);
            return true;
        }


        public async Task<bool> DeleteById<T>(Guid id)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T))); ;
            collection.Delete(id);
            return true;
        }

        public async Task<T> Insert<T>(T entity)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
            collection.Insert(entity);
            return entity;
        }

        public async Task<T> Update<T>(T entity)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T))); ;
            collection.Update(entity);
            return await Task.FromResult(entity);
        }

        public void Update<T>(Expression<Func<T, T>> predicate, Expression<Func<T, bool>> expression)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T))); 
            collection.UpdateMany(predicate, expression);
        }

        public async Task<T> GetFirstOrDefaultAsync<T>(Expression<Func<T, bool>> expression)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
            return collection.FindOne(expression);
        }


        public async Task<T> GetByIdAsync<T>(Guid id)
        {
            var collection = _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
            return collection.FindById(id);
        }

        public ILiteCollection<T> GetCollection<T>()
        {
            return _db.GetCollection<T>(NormalizeCollectionString(typeof(T)));
        }

        public void Rebuild()
        {
            _db.Checkpoint();
        }
    }
}
