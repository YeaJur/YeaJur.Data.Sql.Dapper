#region YeaJur.Data.Sql.Dapper 4.0

/***
 * 本代码版权归  侯兴鼎（YeaJur）  所有，All Rights Reserved (C) 2017
 * CLR版本：4.0.30319.42000
 * 唯一标识：e23a8435-d6b0-444f-8d6a-677dc71a1342
 **
 * 所属域：DESKTOP-Q9MAAK4
 * 机器名称：DESKTOP-Q9MAAK4
 * 登录用户：houxi
 * 创建时间：2017/7/26 21:13:47
 * 创建人：侯兴鼎（YeaJur）
 * E_mail：houxingding@hotmail.com
 **
 * 命名空间：YeaJur.Data.Sql.Dapper
 * 类名称：WriteConnectionPool
 * 文件名：WriteConnectionPool
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
    /// 写数据库连接池
    /// </summary>
    public class WriteConnectionPool
    {
        private static WriteConnectionPool _cpool = null; //池管理对象
        private static readonly object _objlock = typeof(WriteConnectionPool); //池管理对象实例
        private readonly int _size = 10; //池中连接数
        private int _useCount = 0; //已经使用的连接数
        private string _connectionStr = ""; //连接字符串
        private static ConcurrentQueue<IDbConnection> _ConcurrenProducts { get; set; }
        private WriteConnectionPool()
        {
            _connectionStr = ConfigurationManager.ConnectionStrings["WriteDbContext"].ConnectionString;
            _size = 100;
            _ConcurrenProducts = new ConcurrentQueue<IDbConnection>();
        }

        /// <summary>
        /// 创建获取连接池对象
        /// </summary>
        public static WriteConnectionPool Pool
        {
            get
            {
                lock (_objlock)
                {
                    return _cpool ?? (_cpool = new WriteConnectionPool());
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
                }
                catch (Exception ex)
                {
                    LogHandler.Log(ELogType.Fatal, GetType().FullName + "获取写数据库连接错误！", ex);
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