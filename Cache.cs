using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using System.Text;

namespace TinySql.Cache
{
    #region interface and base cache classes

    public interface ICacheProvider<TKey, TValue>
    {
        TValue Get(TKey key);
        void Add(TKey key, TValue value);
        bool Remove(TKey key);

        bool IsCached(TKey key);

        int CacheMinutes { get; set; }
        bool UseSlidingCache { get; set; }

    }

    public abstract class ResultCacheProvider : ICacheProvider<SqlBuilder,ResultTable>
    {
        public ResultCacheProvider()
        {
            SetCachePolicy();
        }

        protected void SetCachePolicy()
        {
            if (UseSlidingCache)
            {
                CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration, SlidingExpiration = TimeSpan.FromMinutes(this.CacheMinutes) };
            }
            else
            {
                CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now.AddMinutes((double)CacheMinutes), SlidingExpiration = MemoryCache.NoSlidingExpiration };
            }
        }

        private string GetKey(SqlBuilder builder)
        {
            return builder.ToSql().GetHashCode().ToString();
        }

        private CacheItemPolicy CachePolicy = null;



        public ResultTable Get(SqlBuilder Builder)
        {
            string key = GetKey(Builder);
            if (MemoryCache.Default.Contains(key))
            {
                return (ResultTable)MemoryCache.Default.Get(key);
            }
            else
            {
                return null;
            }
        }

        public void Add(SqlBuilder Builder, ResultTable Result)
        {
            string key = GetKey(Builder);
            CacheItem item = new CacheItem(key, Result);
            MemoryCache.Default.Add(item, CachePolicy);
        }

        public bool Remove(SqlBuilder Builder)
        {
            try
            {
                string key = GetKey(Builder);
                MemoryCache.Default.Remove(key);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool IsCached(SqlBuilder Builder)
        {
            string key = GetKey(Builder);
            return MemoryCache.Default.Contains(key);
        }

        private int _CacheMinutes = 2;
        public int CacheMinutes
        {
            get
            {
                return _CacheMinutes;
            }
            set
            {
                _CacheMinutes = value;
            }
        }

        private bool _UseSlidingCache = true;

        public bool UseSlidingCache
        {
            get
            {
                return _UseSlidingCache;
            }
            set
            {
                _UseSlidingCache = value;
            }
        }
    }

    

    

    #endregion



    #region Default providers

    public class DefaultResultCacheProvider : ResultCacheProvider
    {

    }


    #endregion


    public sealed class CacheProvider
    {
        #region ctor
       
        private static CacheProvider instance = null;
        private CacheProvider()
        {

        }

        public static CacheProvider Default
        {
            get
            {
                if (instance == null)
                {
                    instance = new CacheProvider();
                }
                return instance;
            }
        }
        #endregion
        
        #region Result cache

        private static bool _UseResultCache = false;

        public static bool UseResultCache
        {
            get { return CacheProvider._UseResultCache; }
            set { CacheProvider._UseResultCache = value; }
        }

        private static ResultCacheProvider ResultCacheInstance = null;
        public static ResultCacheProvider ResultCache
        {
            get
            {
                if (!UseResultCache)
                {
                    throw new InvalidOperationException("The Result Cache is not online. Set 'UseResultCache' to 'true'");
                }

                if (ResultCacheInstance == null)
                {
                    ResultCacheInstance = new DefaultResultCacheProvider();
                }

                return ResultCacheInstance;
            }
            set
            {
                ResultCacheInstance = value;
            }
        }

        #endregion

    }







}
