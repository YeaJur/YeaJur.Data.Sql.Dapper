#region YeaJur.Data.Sql.Dapper 4.0

/***
 * 本代码版权归  侯兴鼎（YeaJur）  所有，All Rights Reserved (C) 2017
 * CLR版本：4.0.30319.42000
 * 唯一标识：517a5922-a659-49dd-94be-5d288cd8f0ba
 **
 * 所属域：DESKTOP-Q9MAAK4
 * 机器名称：DESKTOP-Q9MAAK4
 * 登录用户：houxi
 * 创建时间：2017/7/26 21:08:44
 * 创建人：侯兴鼎（YeaJur）
 * E_mail：houxingding@hotmail.com
 **
 * 命名空间：YeaJur.Data.Sql.Dapper
 * 类名称：DbContext
 * 文件名：DbContext
 * 文件描述：
 ***/

#endregion

using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using YeaJur.Core.Enums;
using YeaJur.Core.Handlers;

namespace YeaJur.Data.Sql.Dapper
{
    /// <summary>
    /// Dapper 数据库上下文
    /// </summary>
    public class DbContext
    {
        private static readonly string writeDbContext =
            ConfigurationManager.ConnectionStrings["WriteDbContext"].ConnectionString;

        private static readonly string readDbContext =
            ConfigurationManager.ConnectionStrings["ReadDbContext"].ConnectionString;

        /// <summary>
        /// 读取数据库上下文
        /// </summary>
        public static IDbConnection ReadDbContext
        {
            get
            {
                IDbConnection dbConnection;
                if (!string.IsNullOrEmpty(readDbContext))
                    dbConnection = new SqlConnection(readDbContext);
                else
                {
                    LogHandler.Log(ELogType.Error, "配置文件中无读数据库连接【ReadDbContext】配置！",
                        new ArgumentNullException("ReadDbContext"));
                    return null;
                }

                var isClosed = dbConnection.State == ConnectionState.Closed;
                if (isClosed) dbConnection.Open();
                return dbConnection;
            }
        }

        /// <summary>
        /// 写数据库上下文
        /// </summary>
        public static IDbConnection WriteDbContext
        {
            get
            {
                IDbConnection dbConnection;
                if (!string.IsNullOrEmpty(writeDbContext))
                    dbConnection = new SqlConnection(writeDbContext);
                else
                {
                    LogHandler.Log(ELogType.Error, "配置文件中无写数据库连接【WriteDbContext】配置！",
                        new ArgumentNullException("writeDbContext"));
                    return null;
                }

                var isClosed = dbConnection.State == ConnectionState.Closed;
                if (isClosed) dbConnection.Open();
                return dbConnection;
            }
        }
    }
}