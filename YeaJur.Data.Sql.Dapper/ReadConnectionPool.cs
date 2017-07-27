#region YeaJur.Data.Sql.Dapper 4.0

/***
 * 本代码版权归  侯兴鼎（YeaJur）  所有，All Rights Reserved (C) 2017
 * CLR版本：4.0.30319.42000
 * 唯一标识：3470a30e-debc-43bc-9940-a66bd7c3ea8b
 **
 * 所属域：DESKTOP-Q9MAAK4
 * 机器名称：DESKTOP-Q9MAAK4
 * 登录用户：houxi
 * 创建时间：2017/7/26 21:12:17
 * 创建人：侯兴鼎（YeaJur）
 * E_mail：houxingding@hotmail.com
 **
 * 命名空间：YeaJur.Data.Sql.Dapper
 * 类名称：ReadConnectionPool
 * 文件名：ReadConnectionPool
 * 文件描述：
 ***/

#endregion

using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using YeaJur.Core.Enums;
using YeaJur.Core.Handlers;

namespace YeaJur.Data.Sql.Dapper
{
    /// <summary>
    /// 读数据库连接池
    /// </summary>
    public class ReadConnectionPool
    {
        private static ReadConnectionPool _cpool; //池管理对象
        private static readonly object Objlock = typeof(ReadConnectionPool); //池管理对象实例
        private readonly int _size; //池中连接数
        private readonly string _connectionStr; //连接字符串
        private int _useCount = 0; //已经使用的连接数
        private static ConcurrentQueue<IDbConnection> _ConcurrenProducts { get; set; }

        private ReadConnectionPool()
        {
            _connectionStr = ConfigurationManager.ConnectionStrings["ReadDbContext"].ConnectionString;
            _size = 100;
            _ConcurrenProducts = new ConcurrentQueue<IDbConnection>();
            Parallel.For(0, 10, (i) =>
            {
                IDbConnection conn2 = new SqlConnection(_connectionStr);
                try
                {
                    conn2.Open();
                    _ConcurrenProducts.Enqueue(conn2);
                }
                catch (Exception ex)
                {
                    LogHandler.Log(ELogType.Fatal, GetType().FullName + "获取读数据库连接错误！", ex);
                    throw ex;
                }
            });
        }

        /// <summary>
        /// 创建获取连接池对象
        /// </summary>
        public static ReadConnectionPool Pool
        {
            get
            {
                lock (Objlock)
                {
                    return _cpool ?? (_cpool = new ReadConnectionPool());
                }
            }
        }

        /// <summary>
        /// 获取池中的连接
        /// </summary>
        public IDbConnection Connection
        {
            get
            {
                IDbConnection tmp;
                _ConcurrenProducts.TryDequeue(out tmp);
                if (tmp != null)
                {
                    _useCount++;
                    return tmp;
                }
                try
                {
                    Parallel.Invoke(() =>
                    {
                        IDbConnection conn2 = new SqlConnection(_connectionStr);
                        conn2.Open();
                        tmp = conn2;
                    });
                    Parallel.For(0, 5, (i) =>
                    {
                        IDbConnection conn2 = new SqlConnection(_connectionStr);
                        conn2.Open();
                        _ConcurrenProducts.Enqueue(conn2);
                    });
                }
                catch (Exception ex)
                {
                    LogHandler.Log(ELogType.Fatal, GetType().FullName + "获取读数据库连接错误！", ex);
                    throw ex;
                }
                return tmp;
            }
        }

        /// <summary>
        /// 关闭连接,加连接回到池中
        /// </summary>
        /// <param name="con"></param>
        public void CloseConnection(IDbConnection con)
        {
            if (con == null) return;
            Parallel.Invoke(() =>
            {
                if (_useCount >= _size)
                {
                    con.Close();
                }
                else
                {
                    _ConcurrenProducts.Enqueue(con);
                    _useCount--;
                }
            });
        }
    }
}