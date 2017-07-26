#region YeaJur.Data.Sql.Dapper 4.0

/***
 * 本代码版权归  侯兴鼎（YeaJur）  所有，All Rights Reserved (C) 2017
 * CLR版本：4.0.30319.42000
 * 唯一标识：738cecfb-83ad-4814-ac2b-99f81d1ba748
 **
 * 所属域：DESKTOP-Q9MAAK4
 * 机器名称：DESKTOP-Q9MAAK4
 * 登录用户：houxi
 * 创建时间：2017/7/26 21:06:42
 * 创建人：侯兴鼎（YeaJur）
 * E_mail：houxingding@hotmail.com
 **
 * 命名空间：YeaJur.Data.Sql.Dapper
 * 类名称：DbHandler
 * 文件名：DbHandler
 * 文件描述：
 ***/

#endregion

using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using Dapper.Contrib.Extensions;
using System.Linq;
using YeaJur.Core.Models;
using YeaJur.Core.Handlers;
using YeaJur.Core.Enums;

namespace YeaJur.Data.Sql.Dapper
{
    /// <summary>
    /// 数据更新基本操作类
    /// </summary>
    /// <typeparam name="TModel">表实体类</typeparam>
    public class DbHandler<TModel> where TModel : class
    {

        #region WriteDB

        /// <summary>
        /// 添加数据信息
        /// </summary>
        /// <param name="model">实体</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Add(TModel model, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (model == null) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    dbWrite.Insert(model);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    dbWrite.Insert(model, transaction);
                }
                return new MResult<object>(MResultMsg.AddSuccess, null, true);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.AddFail, ex);
                return new MResult<object>(MResultMsg.AddFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 批量添加数据信息
        /// </summary>
        /// <param name="list">实体集合</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Add(IEnumerable<TModel> list, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (!list.Any()) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    dbWrite.Insert(list);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    dbWrite.Insert(list, transaction);
                }
                return new MResult<object>(MResultMsg.AddSuccess, null, true);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.AddFail, ex);
                return new MResult<object>(MResultMsg.AddFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 更新数据信息
        /// </summary>
        /// <param name="model">实体</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Update(TModel model, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (model == null) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                bool result;
                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    result = dbWrite.Update(model);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    result = dbWrite.Update(model, transaction);
                }
                return new MResult<object>(result
               ? MResultMsg.UpdateSuccess
               : MResultMsg.UpdateFail, null, result);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.UpdateFail, ex);
                return new MResult<object>(MResultMsg.UpdateFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 批量更新数据信息
        /// </summary>
        /// <param name="list">实体集合</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Update(IEnumerable<TModel> list, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (!list.Any()) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                bool result;
                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    result = dbWrite.Update(list);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    result = dbWrite.Update(list, transaction);
                }
                return new MResult<object>(result
               ? MResultMsg.UpdateSuccess
               : MResultMsg.UpdateFail, null, result);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.UpdateFail, ex);
                return new MResult<object>(MResultMsg.UpdateFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 删除数据信息
        /// </summary>
        /// <param name="model">实体</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Delete(TModel model, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (model == null) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                bool result;
                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    result = dbWrite.Delete(model);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    result = dbWrite.Delete(model, transaction);
                }
                return new MResult<object>(result
               ? MResultMsg.DeleteSuccess
               : MResultMsg.DeleteFail, null, result);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.DeleteFail, ex);
                return new MResult<object>(MResultMsg.DeleteFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 批量删除数据信息
        /// </summary>
        /// <param name="list">实体集合</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public MResult<object> Delete(IEnumerable<TModel> list, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                if (!list.Any()) return new MResult<object>(MResultMsg.DataIsNull, null, false);

                bool result;
                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    result = dbWrite.Delete(list);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                }
                else
                {
                    result = dbWrite.Delete(list, transaction);
                }
                return new MResult<object>(result
                   ? MResultMsg.DeleteSuccess
                   : MResultMsg.DeleteFail, null, result);
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.DeleteFail, ex);
                return new MResult<object>(MResultMsg.DeleteFail + ex.Message, null, false);
            }
        }

        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <param name="dbWrite">写数据库连接</param>
        /// <param name="transaction">事物</param>
        /// <returns></returns>
        public int Execute(string sql, IDbConnection dbWrite = null, IDbTransaction transaction = null)
        {
            try
            {
                int result;
                if (dbWrite == null)
                {
                    dbWrite = WriteConnectionPool.Pool.Connection;
                    result = dbWrite.Execute(sql);
                    WriteConnectionPool.Pool.CloseConnection(dbWrite);
                    return result;
                }
                //dbWrite.CreateCommand().Transaction = transaction;
                //result= dbWrite.Execute(sql);
                IDbCommand command = dbWrite.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = sql;
                result = command.ExecuteNonQuery();
                return result;
            }
            catch (Exception ex)
            {
                WriteConnectionPool.Pool.CloseConnection(dbWrite);
                LogHandler.Log(ELogType.Error, MResultMsg.UpdateFail, ex);
                return 0;
            }
        }

        #endregion

        #region ReadDB

        /// <summary>
        /// 获取实体数据信息
        /// </summary>
        /// <param name="id">主键</param>
        /// <returns></returns>
        public TModel GetModel(object id)
        {
            var dbRead = ReadConnectionPool.Pool.Connection;
            try
            {
                var result = dbRead.Get<TModel>(id);
                ReadConnectionPool.Pool.CloseConnection(dbRead);
                return result;
            }
            catch (Exception ex)
            {
                ReadConnectionPool.Pool.CloseConnection(dbRead);
                LogHandler.Log(ELogType.Error, MResultMsg.QueryFail, ex);
                return null;
            }
        }

        /// <summary>
        /// 获取实体所有数据信息列表
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TModel> GetAll()
        {
            var dbRead = ReadConnectionPool.Pool.Connection;
            try
            {
                var result = dbRead.GetAll<TModel>();
                ReadConnectionPool.Pool.CloseConnection(dbRead);
                return result;
            }
            catch (Exception ex)
            {
                ReadConnectionPool.Pool.CloseConnection(dbRead);
                LogHandler.Log(ELogType.Error, MResultMsg.QueryFail, ex);
                return new List<TModel>();
            }

        }

        /// <summary>
        /// 查询数据列表
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sql, object param = null, CommandType? commandType = null)
        {
            var dbRead = ReadConnectionPool.Pool.Connection;
            try
            {
                var result = dbRead.Query<T>(sql, param, null, true, null, commandType);

                ReadConnectionPool.Pool.CloseConnection(dbRead);
                return result;
            }
            catch (Exception ex)
            {
                ReadConnectionPool.Pool.CloseConnection(dbRead);
                LogHandler.Log(ELogType.Error, MResultMsg.QueryFail, ex);
                return new List<T>();
            }
        }

        #endregion

    }
}