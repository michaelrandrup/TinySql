using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using System.Text;

namespace TinySql.Cache
{
    public interface IResultCacheProvider
    {
        ResultTable ResultFromCahce(SqlBuilder Builder);
        void AddToCache(SqlBuilder Builder, ResultTable Result);
        bool RemoveFromCache(SqlBuilder Builder);

        bool IsCached(SqlBuilder Builder);

        int CacheMinutes { get; set; }
        bool UseSlidingCache { get; set; }

    }

    public abstract class ResultCacheProviderBase : IResultCacheProvider
    {
        public ResultCacheProviderBase()
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



        public ResultTable ResultFromCahce(SqlBuilder Builder)
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

        public void AddToCache(SqlBuilder Builder, ResultTable Result)
        {
            string key = GetKey(Builder);
            CacheItem item = new CacheItem(key, Result);
            MemoryCache.Default.Add(item, CachePolicy);
        }

        public bool RemoveFromCache(SqlBuilder Builder)
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


    public class DefaultResultCacheProvider : ResultCacheProviderBase
    {

    }


    public class CacheProvider
    {
        private static bool _UseResultCache = false;

        public static bool UseResultCache
        {
            get { return CacheProvider._UseResultCache; }
            set { CacheProvider._UseResultCache = value; }
        }

        

        private static IResultCacheProvider instance = null;
        public static IResultCacheProvider ResultCache
        {
            get
            {
                if (!UseResultCache)
                {
                    throw new InvalidOperationException("The Result Cache is not online. Set 'UseResultCache' to 'true'");
                }

                if (instance == null)
                {
                    instance = new DefaultResultCacheProvider();
                }

                return instance;
            }
            set
            {
                instance = value;
            }
        }


    }







}
