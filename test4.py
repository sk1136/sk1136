using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using Portal.Api.Models;
using Portal.Libs.Coverage;
using Portal.Libs.MarketData;
using Portal.Libs.Positions;
using Portal.Libs.Research.EV;
using Portal.Libs.Research.Preview;

namespace Portal.Api.Helpers
{
    public static class DatabaseHelper
    {
        private static Logger Logger = LogManager.GetCurrentClassLogger();

        public static IEnumerable<dynamic> ExecuteStoredProcedureWithResultSet(string DatabaseConnectionString, string StoredProcedure, List<SqlParameter> SqlParams)
        {
            using (var connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandTimeout = Startup.SqlTimeout; 
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = StoredProcedure;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                using (var dataReader = command.ExecuteReader())
                {
                    var fields = new List<String>();

                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        fields.Add(dataReader.GetName(i));
                    }

                    while (dataReader.Read())
                    {
                        var item = new ExpandoObject() as IDictionary<String, Object>;                        

                        for (var i = 0; i < fields.Count; i++)
                        {
                            item.Add(fields[i], dataReader[fields[i]]);
                        }

                        yield return item;
                    }
                }
            }
        }

        public static IEnumerable<dynamic> ExecuteInlineQueryWithResultSet(string DatabaseConnectionString, string Query)
        {
            using (var connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandType = CommandType.Text;
                command.CommandText = Query;

                using (var dataReader = command.ExecuteReader())
                {
                    var fields = new List<String>();

                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        fields.Add(dataReader.GetName(i));
                    }

                    while (dataReader.Read())
                    {
                        var item = new ExpandoObject() as IDictionary<String, Object>;

                        for (var i = 0; i < fields.Count; i++)
                        {
                            item.Add(fields[i], dataReader[fields[i]]);
                        }

                        yield return item;
                    }
                }
            }
        }

        public static async Task<IEnumerable<dynamic>> ExecuteInlineQueryAsyncWithResultSet(DbConnection conn, string query)
        {
            List<dynamic> results = new List<dynamic>();
            var command = conn.CreateCommand();
            command.CommandTimeout = Startup.SqlTimeout;
            command.CommandType = CommandType.Text;
            command.CommandText = query;

            using (var dataReader = await command.ExecuteReaderAsync())
            {
                var fields = new List<string>();

                for (var i = 0; i < dataReader.FieldCount; i++)
                {
                    fields.Add(dataReader.GetName(i));
                }

                while (dataReader.Read())
                {
                    var item = new ExpandoObject() as IDictionary<string, object>;

                    for (var i = 0; i < fields.Count; i++)
                    {
                        item.Add(fields[i], dataReader[fields[i]]);
                    }

                    results.Add(item);
                }
                return results;
            }
        }

        public static async Task<IEnumerable<dynamic>> ExecuteInlineQueryAsyncWithResultSet(string DatabaseConnectionString, string Query, List<SqlParameter> SqlParams = null)
        {
            List<dynamic> results = new List<dynamic>();
            using (var connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandType = CommandType.Text;
                command.CommandText = Query;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                using (var dataReader = await command.ExecuteReaderAsync())
                {
                    var fields = new List<String>();

                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        fields.Add(dataReader.GetName(i));
                    }

                    while (dataReader.Read())
                    {
                        var item = new ExpandoObject() as IDictionary<String, Object>;

                        for (var i = 0; i < fields.Count; i++)
                        {
                            item.Add(fields[i], dataReader[fields[i]]);
                        }

                        results.Add(item);
                    }

                    return results;
                }
            }
        }

        public static async Task<object> ExecuteInlineQueryAsyncReturnValue(DbConnection conn, string query, List<SqlParameter> SqlParams = null)
        {
            var command = conn.CreateCommand();
            command.CommandTimeout = Startup.SqlTimeout;
            command.CommandType = CommandType.Text;
            command.CommandText = query;

            if (SqlParams != null)
            {
                foreach (SqlParameter param in SqlParams)
                {
                    command.Parameters.Add(param);
                }
            }


            using (var dataReader = await command.ExecuteReaderAsync())
            {
                var fields = new List<string>();

                for (var i = 0; i < dataReader.FieldCount; i++)
                {
                    fields.Add(dataReader.GetName(i));
                }

                return command.ExecuteScalar();
            }
        }

        public static object ExecuteInlineQueryWithReturnValue(
            string DatabaseConnectionString, string Query, List<SqlParameter> SqlParams = null)
        {
            object ReturnValue;

            using (var connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandType = CommandType.Text;
                command.CommandText = Query;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                ReturnValue = command.ExecuteScalar();
            }

            return ReturnValue;
        }

        public static int? ExecuteStoredProcedureWithReturnValue(string DatabaseConnectionString, string StoredProcedure, List<SqlParameter> SqlParams)
        {
            int? ReturnValue = null;

            using (SqlConnection connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandText = StoredProcedure;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                ReturnValue = (int)command.ExecuteScalar();
            }

            return ReturnValue;
        }

        public static object ExecuteStoredProcedureWithReturnValueO(string DatabaseConnectionString, string StoredProcedure, List<SqlParameter> SqlParams)
        {
            object ReturnValue = null;

            using (SqlConnection connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandText = StoredProcedure;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                ReturnValue = command.ExecuteScalar();
            }

            return ReturnValue;
        }

        public static int? ExecuteStoredProcedure(string DatabaseConnectionString, string StoredProcedure, List<SqlParameter> SqlParams)
        {
            int? ReturnValue = null;

            using (SqlConnection connection = new SqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.CommandTimeout = Startup.SqlTimeout;
                command.CommandText = StoredProcedure;

                if (SqlParams != null)
                {
                    foreach (SqlParameter param in SqlParams)
                    {
                        command.Parameters.Add(param);
                    }
                }

                ReturnValue = command.ExecuteNonQuery();
            }

            return ReturnValue;
        }

        public static IEnumerable<ExpectedValueIdeaRecommendation> GetExpectedValueIdeaRecommendations()
        {
            List<ExpectedValueIdeaRecommendation> ReturnValue = new List<ExpectedValueIdeaRecommendation>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                    (Startup.HoloceneDatabaseConnectionString,
                    "research.GetExpectedValueIdeaRecommendations", null);

            foreach (dynamic row in reader)
            {
                var rec = new ExpectedValueIdeaRecommendation
                {
                    ExpectedValueIdeaRecommendationId = row.ExpectedValueIdeaRecommendationId,
                    Code = row.Code,
                    Desc = row.Desc
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<EarningsPreviewReference> GetEarningsPreviewReference()
        {
            List<EarningsPreviewReference> ReturnValue = new List<EarningsPreviewReference>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "research.GetEarningsPreviewReference", null);

            foreach (dynamic row in reader)
            {
                var earningsPreview = new EarningsPreviewReference
                {
                    EarningsPreviewReferenceId = row.EarningsPreviewReferenceId,
                    RefGroup = row.RefGroup,
                    RefName = row.RefName,
                    RefValue = row.RefValue
                };

                ReturnValue.Add(earningsPreview);
            }

            return ReturnValue.AsEnumerable();
        }


        public static IEnumerable<EarningsPreview_Old> GetEarningsPreview(string Symbol, string Username)
        {
            List<EarningsPreview_Old> ReturnValue = new List<EarningsPreview_Old>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }
            parameters.Add(new SqlParameter("@Username", Username));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetEarningsPreview", parameters);

            foreach (dynamic row in reader)
            {
                var earningsPreview = new EarningsPreview_Old
                {
                    EarningsPreviewId = row.EarningsPreviewId,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    ShortName = row.ShortName,
                    CINS = row.Cins,
                    CUSIP = row.Cusip,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    Thesis = row.Thesis,
                    Estimates = row.Estimates,
                    BusDate = row.BusDate,
                    Timestamp = row.ModifiedOn
                };

                ReturnValue.Add(earningsPreview);
            }

            return ReturnValue.AsEnumerable();
        }
        public static IEnumerable<FirmCoverage> GetFirmCoverageForTmT(string Symbol)
        {
            List<FirmCoverage> ReturnValue = new List<FirmCoverage>();

            IEnumerable<dynamic> reader = 
                ExecuteInlineQueryWithResultSet(Startup.HoloceneDatabaseConnectionString,
                $@"select Symbol from analyst.FirmCoverage (nolock) where Symbol = '{Symbol}' and Sector = 'TMT'");
            
            
            foreach (dynamic row in reader)
            {
                var firmCoverage = new FirmCoverage
                {
                    Symbol = row.Symbol,
                };

                ReturnValue.Add(firmCoverage);
            }
            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<EarningsThesis> GetEarningsThesis(string Symbol, string Username, int EarningsThesisId)
        {
            List<EarningsThesis> ReturnValue = new List<EarningsThesis>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (EarningsThesisId != 0)
            {
                parameters.Add(new SqlParameter("@EarningsThesisId", EarningsThesisId));
            }
            else
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
                parameters.Add(new SqlParameter("@Username", Username));
            }

            IEnumerable<dynamic> reader = 
                ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetEarningsThesis", parameters);

            foreach (dynamic row in reader)
            {
                var earningsThesis = new EarningsThesis
                {
                    EarningsThesisId = row.EarningsThesisId,
                    BusDate = row.BusDate,
                    Summary = row.Summary,
                    Thesis = row.Thesis,
                    Recommendation = row.Recommendation,
                    Timestamp = row.ModifiedOn,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    Status = row.Status,
                    ExpectedValueId = row.ExpectedValueId,
                    EV = row.EV,
                    ER = row.ER,
                    SEMV = row.SEMV,
                    SEMVFund = row.SEMVFund,
                    EVCQF = row.EVCQF,
                    Filename = row.Filename,
                    EarningsDate = (row.EarningsDate is System.DBNull ? null : row.EarningsDate),
                    EVTimestamp = (row.EVTimestamp is System.DBNull ? null : row.EVTimestamp)
                };

                ReturnValue.Add(earningsThesis);
            }

            return ReturnValue.AsEnumerable();
        }

        public static bool IsPropertyExist(dynamic settings, string name)
        {
            if (settings is ExpandoObject)
                return ((IDictionary<string, object>)settings).ContainsKey(name);

            return settings.GetType().GetProperty(name) != null;
        }


        public static ExpectedValue GetExpectedValueById(Int64 ExpectedValueId)
        {
            ExpectedValue ReturnValue = new ExpectedValue();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@ExpectedValueId", ExpectedValueId));

            IEnumerable<dynamic> reader = 
                 ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "research.GetExpectedValueById", parameters);

            foreach (dynamic row in reader)
            {
                ReturnValue = new ExpectedValue
                {
                    ExpectedValueId = row.ExpectedValueId,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    ShortName = row.ShortName,
                    CINS = row.Cins,
                    CUSIP = row.Cusip,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    MetricTypeCode = row.MetricTypeCode,
                    PeriodTypeCode = row.PeriodTypeCode,
                    PeriodTypeDesc = row.PeriodTypeDesc,
                    IdeaRecommendationCode = row.IdeaRecommendationCode,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc,
                    OverrideCalc = Convert.ToBoolean(row.OverrideCalc),
                    Price = row.Price,
                    Upside = row.Upside,
                    BusDate = row.BusDate,
                    IsLocked = row.IsLocked,
                    Timestamp = row.ModifiedOn
                };
            }

            return ReturnValue;
        }

        public static IEnumerable<ExpectedValue> GetExpectedValues(List<int> IDs)
        {
            List<ExpectedValue> ReturnValue = new List<ExpectedValue>();
            if (IDs.Count == 0)
                return ReturnValue;

            string IDsCSV = string.Join(",", IDs);

            IEnumerable<dynamic> reader = 
                    ExecuteInlineQueryWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "select a.ExpectedValueId,a.BusDate,a.ModifiedOn,a.Price,a.Upside,a.ModifiedBy,d.Symbol,d.FullBloombergCode,b.DisplayName,b.Username,c.MetricTypeCode,coalesce(f.IdeaRecommendationCode,'') IdeaRecommendationCode,coalesce(a.OverrideCalc,0) OverrideCalc," +
                    "coalesce(f.IdeaRecommendationDesc,'') IdeaRecommendationDesc from research.ExpectedValue (nolock) a " +
                    "join app.[User] (nolock) b on (a.UserId = b.UserId) " +
                    "join research.ExpectedValueMetricType (nolock) c on (a.MetricTypeId = c.ExpectedValueMetricTypeId) " +
                    "join sm.[Security] (nolock) d on (a.SecurityId = d.SecurityId) " +
                    "left join research.ExpectedValuePeriodType (nolock) e on (a.PeriodTypeId = e.ExpectedValuePeriodTypeId) " +
                    "left join research.ExpectedValueIdeaRecommendation (nolock) f on (a.IdeaRecommendationId = f.ExpectedValueIdeaRecommendationId) " +
                    "where a.ExpectedValueId in (" + IDsCSV + ")"
                    );

            foreach (dynamic row in reader)
            {
                var expectedValue = new ExpectedValue
                {
                    ExpectedValueId = row.ExpectedValueId,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    MetricTypeCode = row.MetricTypeCode,
                    OverrideCalc = Convert.ToBoolean(row.OverrideCalc),
                    IdeaRecommendationCode = row.IdeaRecommendationCode,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc,
                    Price = row.Price,
                    Upside = row.Upside,
                    BusDate = row.BusDate,
                    Timestamp = row.ModifiedOn
                };

                ReturnValue.Add(expectedValue);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<ExpectedValue> GetExpectedValuesLatestByUserAndTicker()
        {
            List<ExpectedValue> ReturnValue = new List<ExpectedValue>();

            IEnumerable<dynamic> reader = ExecuteInlineQueryWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "select UserId,SecurityId,max(ExpectedValueId) ExpectedValueId from research.ExpectedValue (nolock) " +
                    "group by UserId,SecurityId");

            foreach (dynamic row in reader)
            {
                var ev = new ExpectedValue
                {
                    ExpectedValueId = row.ExpectedValueId
                };

                ReturnValue.Add(ev);
            }

            return ReturnValue.AsEnumerable();
        }

        public static bool CreateIdea(Idea Idea, string Username)
        {
            bool ReturnValue = true;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Idea.Symbol },
                    new SqlParameter() {ParameterName = "@SecurityDesc", SqlDbType = SqlDbType.NVarChar, Value = Idea.SecurityDesc },
                    new SqlParameter() {ParameterName = "@Assignee", SqlDbType = SqlDbType.NVarChar, Value = Idea.Assignee },
                    new SqlParameter() {ParameterName = "@Analyst", SqlDbType = SqlDbType.NVarChar, Value = Idea.Analyst },
                    new SqlParameter() {ParameterName = "@IdeaStatusCode", SqlDbType = SqlDbType.NVarChar, Value = Idea.IdeaStatusCode },
                    new SqlParameter() {ParameterName = "@Thesis", SqlDbType = SqlDbType.NVarChar, Value = Idea.Thesis },
                    new SqlParameter() {ParameterName = "@IdeaSource", SqlDbType = SqlDbType.NVarChar, Value = Idea.IdeaSource },
                    new SqlParameter() {ParameterName = "@ModelCompleted", SqlDbType = SqlDbType.Bit, Value = Idea.ModelCompleted },
                    new SqlParameter() {ParameterName = "@IdeaId", SqlDbType = SqlDbType.Int, Value = Idea.IdeaId, Direction = ParameterDirection.InputOutput }
                };

            ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString, "research.CreateIdea", parameters);

            return ReturnValue;
        }

        public static bool UpdateIdea(Idea Idea, string Username)
        {
            bool ReturnValue = true;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Analyst", SqlDbType = SqlDbType.NVarChar, Value = Idea.Analyst },
                    new SqlParameter() {ParameterName = "@Thesis", SqlDbType = SqlDbType.NVarChar, Value = Idea.Thesis },
                    new SqlParameter() {ParameterName = "@IdeaSource", SqlDbType = SqlDbType.NVarChar, Value = Idea.IdeaSource },
                    new SqlParameter() {ParameterName = "@IdeaStatusCode", SqlDbType = SqlDbType.NVarChar, Value = Idea.IdeaStatusCode },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@ModelCompleted", SqlDbType = SqlDbType.Bit, Value = Idea.ModelCompleted },
                    new SqlParameter() {ParameterName = "@IdeaId", SqlDbType = SqlDbType.Int, Value = Idea.IdeaId, Direction = ParameterDirection.Input }
                };

            ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString, "research.UpdateIdea", parameters);

            return ReturnValue;
        }

        public static bool UpsertPrice(MarketData MktData)
        {
            bool ReturnValue = true;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = MktData.Symbol },
                    new SqlParameter() {ParameterName = "@Price", SqlDbType = SqlDbType.NVarChar, Value = MktData.PX_LAST },
                    new SqlParameter() {ParameterName = "@BusDate", SqlDbType = SqlDbType.DateTime, Value = DateTime.Now.ToString("yyyy/MM/dd") }
                };

            ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString, "sm.UpsertPrice", parameters);

            return ReturnValue;
        }

        public static bool UpdateIdeaAbstract(IdeaAbstract IdeaAbstract)
        {
            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.Symbol },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.Username },
                    new SqlParameter() {ParameterName = "@LongTermViewCode", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.LongTermViewCode },
                    new SqlParameter() {ParameterName = "@ShortTermViewCode", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.ShortTermViewCode },
                    new SqlParameter() {ParameterName = "@LongTermThesis", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.LongTermThesis },
                    new SqlParameter() {ParameterName = "@ShortTermThesis", SqlDbType = SqlDbType.NVarChar, Value = IdeaAbstract.ShortTermThesis }
                };

            ExecuteStoredProcedure
                (Startup.HoloceneDatabaseConnectionString, "research.InsertIdeaAbstract", parameters);

            return true;
        }

        public static bool SubmitEarningsThesis(string Symbol, string Username, string Summary, string Thesis, string Recommendation, int ExpectedValueId, 
            string Status, DateTime EarningsDate, decimal EV, decimal ER, decimal SEMV, decimal SEMVFund, string Filename)
        {
            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@Summary", SqlDbType = SqlDbType.NVarChar, Value = Summary },
                    new SqlParameter() {ParameterName = "@Thesis", SqlDbType = SqlDbType.NVarChar, Value = Thesis },
                    new SqlParameter() {ParameterName = "@Recommendation", SqlDbType = SqlDbType.NVarChar, Value = Recommendation },
                    new SqlParameter() {ParameterName = "@Status", SqlDbType = SqlDbType.NVarChar, Value = Status },
                    new SqlParameter() {ParameterName = "@ExpectedValueId", SqlDbType = SqlDbType.Int, Value = ExpectedValueId },
                    new SqlParameter() {ParameterName = "@EV", SqlDbType = SqlDbType.Decimal, Value = EV },
                    new SqlParameter() {ParameterName = "@ER", SqlDbType = SqlDbType.Decimal, Value = ER },
                    new SqlParameter() {ParameterName = "@SEMV", SqlDbType = SqlDbType.Decimal, Value = SEMV },
                    new SqlParameter() { ParameterName = "@EarningsDate", SqlDbType = SqlDbType.DateTime, Value = EarningsDate },
                    new SqlParameter() {ParameterName = "@SEMVFund", SqlDbType = SqlDbType.Decimal, Value = SEMVFund },
                    new SqlParameter() {ParameterName = "@Filename", SqlDbType = SqlDbType.NVarChar, Value = Filename }
                };

            ExecuteStoredProcedure(Startup.HoloceneDatabaseConnectionString, "research.SubmitEarningsThesis", parameters);
            return true;
        }

        public static bool UpdateEarningsPreview(EarningsPreview_Old EarningsPreview, string Username)
        {
            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.Symbol },
                    new SqlParameter() {ParameterName = "@SecurityDesc", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.SecurityDesc },
                    new SqlParameter() {ParameterName = "@FullBloombergCode", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.FullBloombergCode },
                    new SqlParameter() {ParameterName = "@ShortName", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.ShortName },
                    new SqlParameter() {ParameterName = "@CUSIP", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.CUSIP },
                    new SqlParameter() {ParameterName = "@CINS", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.CINS },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.Username },
                    new SqlParameter() {ParameterName = "@Thesis", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.Thesis },
                    new SqlParameter() {ParameterName = "@Estimates", SqlDbType = SqlDbType.NVarChar, Value = EarningsPreview.Estimates },
                    new SqlParameter() {ParameterName = "@EarningsPreviewId", SqlDbType = SqlDbType.Int, Value = EarningsPreview.EarningsPreviewId, Direction = ParameterDirection.InputOutput }
                };

            ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString, "research.UpsertEarningsPreview", parameters);
            return true;
        }

        public static IEnumerable<User> GetAnalysts()
        {
            List<User> ReturnValue = new List<User>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "app.GetAnalysts", null);

            foreach (dynamic row in reader)
            {
                var user = new User
                {
                    UserId = row.UserId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    FirstName = row.FirstName,
                    LastName = row.LastName,
                    EmailAddress = row.EmailAddress,
                    IsAnalyst = row.IsAnalyst,
                    IsPM = row.IsPM,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
                ReturnValue.Add(user);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<User> GetUsers()
        {
            List<User> ReturnValue = new List<User>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "app.GetUsers", null);

            foreach (dynamic row in reader)
            {
                var user = new User
                {
                    UserId = row.UserId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    FirstName = row.FirstName,
                    LastName = row.LastName,
                    EmailAddress = row.EmailAddress,
                    IsAnalyst = row.IsAnalyst,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    ReportsToDisplayName = row.ReportsToDisplayName,
                    ReportsToEmailAddress = row.ReportsToEmailAddress,
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
                ReturnValue.Add(user);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<IdeaStatus> GetIdeaStatuses()
        {
            List<IdeaStatus> ReturnValue = new List<IdeaStatus>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetIdeaStatuses", null);

            foreach (dynamic row in reader)
            {
                var status = new IdeaStatus
                {
                    IdeaStatusId = row.IdeaStatusId,
                    StatusCode = row.StatusCode,
                    StatusDesc = row.StatusDesc
                };
                ReturnValue.Add(status);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<ExpectedValueDashboard> GetExpectedValueDashboard(string Username)
        {
            List<ExpectedValueDashboard> ReturnValue = new List<ExpectedValueDashboard>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username } };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetExpectedValueDashboardAll", null);

            foreach (dynamic row in reader)
            {
                var dash = new ExpectedValueDashboard
                {
                    Id = row.Id,
                    SecurityId = row.SecurityId,
                    AnalystId = row.AnalystId,
                    ExpectedValueId = row.ExpectedValueId,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    Position = row.Position,
                    Exposure = row.Exposure,
                    IdioPnl = row.IdioPnl,
                    DTD = row.DTD,
                    MTD = row.MTD,
                    YTD = row.YTD,
                    PortfolioLastModified = (row.PortfolioLastModified is System.DBNull ? null : row.PortfolioLastModified),
                    EVLastModified = (row.EVLastModified is System.DBNull ? null : row.EVLastModified),
                    EVBusDate = (row.EVBusDate is System.DBNull ? null : row.EVBusDate),
                    EV = row.EV,
                    ER = row.ER,
                    EVDaysSinceLastUpdate = row.EVDaysSinceLastUpdate,
                    EVHorizon = row.EVHorizon,
                    CQF = row.CQF,
                    BarraId = row.BarraId,
                    SpecReturn = row.SpecReturn,
                    SpecRisk = row.SpecRisk,
                    ERBySpecRiskScaledToGMV = row.ERBySpecRiskScaledToGMV,
                    PredBeta = row.PredBeta,
                    RealizedAlpha = row.RealizedAlpha,
                    ProjectedAlpha = row.ProjectedAlpha,
                    IsLatestByDesk = Convert.ToBoolean(row.IsLatestByDesk),
                    IsMissingBarraData = Convert.ToBoolean(row.IsMissingBarraData),
                    IsMissingBarraDataForRealizedAlpha = Convert.ToBoolean(row.IsMissingBarraDataForRealizedAlpha),
                    IsMissingEVFromDesk = Convert.ToBoolean(row.IsMissingEVFromDesk),
                    SpecRiskUSD = row.SpecRiskUSD,
                    PredBetaUSD = row.PredBetaUSD,
                    EarningsThesisDaysOld = row.EarningsThesisDaysOld,
                    EarningsThesisId = row.EarningsThesisId
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<EVDashboard> GetEVDashboard()
        {
            List<EVDashboard> ReturnValue = new List<EVDashboard>();

            IEnumerable<dynamic> reader = DatabaseHelper
                    .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "research.GetEVDashboard", null);

            foreach (dynamic row in reader)
            {
                var dash = new EVDashboard
                {
                    SecurityId = row.SecurityId,
                    AnalystId = row.AnalystId,
                    ExpectedValueId = row.ExpectedValueId,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    AnalystCode = row.AnalystCode,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    IdeaRecommendationCode = row.IdeaRecommendationCode,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc,
                    Exposure = Convert.ToDecimal(row.Exposure),
                    DTD = Convert.ToDecimal(row.DTD),
                    IdioPnl = Convert.ToDecimal(row.IdioPnl),
                    PortfolioLastModified = (row.PortfolioLastModified is System.DBNull ? null : row.PortfolioLastModified),
                    EVLastModified = (row.EVLastModified is System.DBNull ? null : row.EVLastModified),
                    EVBusDate = (row.EVBusDate is System.DBNull ? null : row.EVBusDate),
                    EV = Convert.ToDecimal(row.EV),
                    ER = Convert.ToDecimal(row.ER),
                    EVDaysSinceLastUpdate = row.EVDaysSinceLastUpdate,
                    IdeaDaysSinceLastUpdate = row.IdeaDaysSinceLastUpdate,
                    EVHorizon = row.EVHorizon,
                    IsLatestByDesk = Convert.ToBoolean(row.IsLatestByDesk),
                    RealizedAlpha = Convert.ToDecimal(row.RealizedAlpha),
                    ProjectedAlpha = Convert.ToDecimal(row.ProjectedAlpha),

                    BarraId = row.BarraId,
                    IsMissingBarraData = Convert.ToBoolean(row.IsMissingBarraData),
                    IsMissingBarraDataForRealizedAlpha = Convert.ToBoolean(row.IsMissingBarraDataForRealizedAlpha),

                    IsMissingEVFromDesk = Convert.ToBoolean(row.IsMissingEVFromDesk),
                    EarningsThesisDaysOld = row.EarningsThesisDaysOld,
                    EarningsThesisId = row.EarningsThesisId,
                    EarningsThesisFilename = row.EarningsThesisFilename,

                    ResearchEventDate = (row.ResearchEventDate is System.DBNull ? null : row.ResearchEventDate),
                    ResearchEventAnalyst = row.ResearchEventAnalyst,
                    ResearchEventDaysSinceLastUpdate = row.ResearchEventDaysSinceLastUpdate,
                    ResearchEventId = row.ResearchEventId,

                    pBlowout = row.pBlowout,
                    pUpside = row.pUpside,
                    pBase = row.pBase,
                    pDownside = row.pDownside,
                    pDisaster = row.pDisaster,
                    pbBlowout = row.pbBlowout,
                    pbUpside = row.pbUpside,
                    pbBase = row.pbBase,
                    pbDownside = row.pbDownside,
                    pbDisaster = row.pbDisaster,

                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    IdeaAbstractLongTermViewCode = row.IdeaAbstractLongTermViewCode,
                    IdeaAbstractLongTermViewDesc = row.IdeaAbstractLongTermViewDesc,

                    StressScore = (row.StressValueCode is System.DBNull ? string.Empty : row.StressValueCode),
                    StressScoreTooltip = (row.StressValueTooltip is System.DBNull ? string.Empty : row.StressValueTooltip),

                    ARKWeight = (row.ARKWeight is System.DBNull ? null : Convert.ToDecimal(row.ARKWeight)),

                    RedditPopularity = (row.RedditPopularity is System.DBNull ? null : Convert.ToDecimal(row.RedditPopularity)),
                    RedditzScore = new List<decimal?>(),

                    DollarTurnover = (row.DollarTurnover is System.DBNull ? null : Convert.ToDecimal(row.DollarTurnover)),
                    SI_DaysToCover = (row.SI_DaysToCover is System.DBNull ? null : Convert.ToDecimal(row.SI_DaysToCover)),
                    SI_Util = (row.SI_Util is System.DBNull ? null : Convert.ToDecimal(row.SI_Util)),
                    SI_PctFloat = (row.SI_PctFloat is System.DBNull ? null : Convert.ToDecimal(row.SI_PctFloat)),
                    SocialActivityzScoreUniversal = (row.SocialActivityzScoreUniversal is System.DBNull ? null : Convert.ToDecimal(row.SocialActivityzScoreUniversal)),
                    SocialActivityzScoreTrend = (row.SocialActivityzScoreTrend is System.DBNull ? null : Convert.ToDecimal(row.SocialActivityzScoreTrend)),
                    OptionsOpenInterest = (row.OptionsOpenInterest is System.DBNull ? null : Convert.ToDecimal(row.OptionsOpenInterest)),
                    LimitUtil = (row.LimitUtil is System.DBNull ? null : Convert.ToDecimal(row.LimitUtil)),
                    AlertDesc = row.AlertDesc is System.DBNull || string.IsNullOrEmpty(row.AlertDesc) ? null : new List<string>(((string)row.AlertDesc).Split("&")).Select(x => x.Trim()),
                    MktCap = (row.MktCap is System.DBNull ? null : Convert.ToDecimal(row.MktCap)),
                    ThesisId = row.ThesisId,
                    ThesisCreatedOn = row.ThesisCreatedOn is DBNull ? null : row.ThesisCreatedOn,
                    ThesisText = row.ThesisText
                };

                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PnlCompare> GetPnlCompare2(string Timestamp1, string Timestamp2, string Desk)
        {
            List<PnlCompare> ReturnValue = new List<PnlCompare>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            DateTime dateValue = DateTime.MinValue;
            if (!string.IsNullOrEmpty(Timestamp1) && DateTime.TryParse(Timestamp1, out dateValue))
                parameters.Add(new SqlParameter("@StartTimestamp", dateValue));
            dateValue = DateTime.MinValue;
            if (!string.IsNullOrEmpty(Timestamp2) && DateTime.TryParse(Timestamp2, out dateValue))
                parameters.Add(new SqlParameter("@EndTimestamp", dateValue));
            parameters.Add(new SqlParameter("@Desk", Desk));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "core.GetPnlCompare2", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PnlCompare
                {
                    Timestamp1 = (row.Timestamp1 is System.DBNull ? null : row.Timestamp1),
                    Timestamp2 = (row.Timestamp2 is System.DBNull ? null : row.Timestamp2),
                    EntityType = row.EntityType,
                    Entity = row.Entity,
                    DTD1 = Convert.ToDecimal(row.DTD1),
                    DTD2 = Convert.ToDecimal(row.DTD2),
                    DeltaDTD = Convert.ToDecimal(row.DeltaDTD),
                    Idio1 = Convert.ToDecimal(row.Idio1),
                    Idio2 = Convert.ToDecimal(row.Idio2),
                    DeltaIdio = Convert.ToDecimal(row.DeltaIdio)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PnlCompare> GetPnlCompare(string Timestamp1, string Timestamp2, string PnlType)
        {
            List<PnlCompare> ReturnValue = new List<PnlCompare>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            DateTime dateValue = DateTime.MinValue;
            if (!string.IsNullOrEmpty(Timestamp1) && DateTime.TryParse(Timestamp1, out dateValue))
                parameters.Add(new SqlParameter("@StartTimestamp", dateValue));
            dateValue = DateTime.MinValue;
            if (!string.IsNullOrEmpty(Timestamp2) && DateTime.TryParse(Timestamp2, out dateValue))
                parameters.Add(new SqlParameter("@EndTimestamp", dateValue));
            parameters.Add(new SqlParameter("@PnlType", PnlType));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetPnlCompare", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PnlCompare
                {
                    Timestamp1 = (row.Timestamp1 is System.DBNull ? null : row.Timestamp1),
                    Timestamp2 = (row.Timestamp2 is System.DBNull ? null : row.Timestamp2),
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Symbol = row.Symbol,
                    UnderlyingSymbol = row.UnderlyingSymbol,
                    DTD1 = row.DTD1,
                    MTD1 = row.MTD1,
                    YTD1 = row.YTD1,
                    DTD2 = row.DTD2,
                    MTD2 = row.MTD2,
                    YTD2 = row.YTD2,
                    DeltaDTD = row.DeltaDTD,
                    DeltaMTD = row.DeltaMTD,
                    DeltaYTD = row.DeltaYTD,
                    IsDelta = row.IsDelta
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetPositionTimeSeries(string Symbol,string Desk,string Sector,string PositionBlock,string Side)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            if (!string.IsNullOrEmpty(Symbol))
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            if (!string.IsNullOrEmpty(Desk))
                parameters.Add(new SqlParameter("@Desk", Desk));
            if (!string.IsNullOrEmpty(Sector))
                parameters.Add(new SqlParameter("@Sector", Sector));
            if (!string.IsNullOrEmpty(PositionBlock))
                parameters.Add(new SqlParameter("@PositionBlock", PositionBlock));
            if (!string.IsNullOrEmpty(Side))
                parameters.Add(new SqlParameter("@Side", Side));

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetPositionTimeSeries", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new Position
                {
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Symbol = row.Symbol,
                    Side = row.Side,
                    DTD = row.DTD,
                    IdioPnl = row.IdioPnl,
                    MTD = row.MTD,
                    YTD = row.YTD
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetIntradayPositionTimeSeries(string Symbol)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            if (!string.IsNullOrEmpty(Symbol))
                parameters.Add(new SqlParameter("@Symbol", Symbol));

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetIntradayPositionTimeSeries", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new Position
                {
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    Desk = row.Desk,
                    Symbol = row.Symbol,
                    DTD = Convert.ToDecimal(row.DTD),
                    IdioPnl = Convert.ToDecimal(row.IdioPnl)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<string> GetEntitiesByEntityType(string EntityType)
        {
            List<string> ReturnValue = new List<string>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            if (!string.IsNullOrEmpty(EntityType))
                parameters.Add(new SqlParameter("@EntityType", EntityType));

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetEntitiesByEntityType", parameters);

            ReturnValue.Add(string.Empty);
            foreach (dynamic row in reader)
            {
                ReturnValue.Add(row.Entity);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PnlSummary> GetPositionTimeSeriesByEntityType(string EntityType, string Entity)
        {
            List<PnlSummary> ReturnValue = new List<PnlSummary>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            if (!string.IsNullOrEmpty(EntityType))
                parameters.Add(new SqlParameter("@EntityType", EntityType));
            if (!string.IsNullOrEmpty(Entity))
                parameters.Add(new SqlParameter("@Entity", Entity));

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetPositionTimeSeriesByEntityType", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PnlSummary
                {
                    Timestamp = row.PortfolioTimestamp,
                    EntityType = row.EntityType,
                    Entity = row.Entity,
                    DTD = row.DTD,
                    IdioPnl = row.IdioPnl,
                    MTD = row.MTD,
                    YTD = row.YTD
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<ResearchFinancial> GetResearchFinancials(string Symbol)
        {
            List<ResearchFinancial> ReturnValue = new List<ResearchFinancial>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            if (!string.IsNullOrEmpty(Symbol))
                parameters.Add(new SqlParameter("@Symbol", Symbol));

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "research.GetFinancials", parameters);

            foreach (dynamic row in reader)
            {
                var financial = new ResearchFinancial
                {
                    Symbol = row.Symbol,
                    PeriodType = (row.PeriodType is System.DBNull ?
                                        string.Empty : row.PeriodType),
                    PeriodEndDate = (row.PeriodEndDate is System.DBNull ?
                                        null : row.PeriodEndDate),
                    MetricType = (row.MetricType is System.DBNull ?
                                        string.Empty : row.MetricType),
                    Metric = (row.Metric is System.DBNull ?
                                        null : row.Metric),
                    MetricDate = (row.MetricDate is System.DBNull ?
                                        null : row.MetricDate),
                    Analyst = (row.DisplayName is System.DBNull ?
                                        string.Empty : row.DisplayName),
                    ModifiedOn = (row.ModifiedOn is System.DBNull ?
                                        null : row.ModifiedOn),
                    Status = (row.Status is System.DBNull ?
                                        string.Empty : row.Status),
                };
                ReturnValue.Add(financial);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PortfolioConstructionDashboard> GetPortfolioConstructionDashboard(string Username)
        {
            List<PortfolioConstructionDashboard> ReturnValue = new List<PortfolioConstructionDashboard>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetPortfolioConstructionDashboard", null);

            foreach (dynamic row in reader)
            {
                var dash = new PortfolioConstructionDashboard
                {
                    Id = row.Id,
                    SecurityId = row.SecurityId,
                    AnalystId = row.AnalystId,
                    ExpectedValueId = row.ExpectedValueId,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    Position = row.Position,
                    Exposure = row.Exposure,
                    DTD = row.DTD,
                    MTD = row.MTD,
                    YTD = row.YTD,
                    PortfolioLastModified = (row.PortfolioLastModified is System.DBNull ? null : row.PortfolioLastModified),
                    EVLastModified = (row.EVLastModified is System.DBNull ? null : row.EVLastModified),
                    EVBusDate = (row.EVBusDate is System.DBNull ? null : row.EVBusDate),
                    EV = row.EV,
                    ER = row.ER,
                    EVDaysSinceLastUpdate = row.EVDaysSinceLastUpdate,
                    EVHorizon = row.EVHorizon,
                    BarraId = row.BarraId,
                    SpecReturn = row.SpecReturn,
                    SpecRisk = row.SpecRisk,
                    ERBySpecRiskScaledToGMV = row.ERBySpecRiskScaledToGMV,
                    PredBeta = row.PredBeta,
                    RealizedAlpha = row.RealizedAlpha,
                    ProjectedAlpha = row.ProjectedAlpha,
                    IsMissingBarraData = Convert.ToBoolean(row.IsMissingBarraData),
                    IsMissingBarraDataForRealizedAlpha = Convert.ToBoolean(row.IsMissingBarraDataForRealizedAlpha),
                    IsMissingEVFromDesk = Convert.ToBoolean(row.IsMissingEVFromDesk),
                    SpecRiskUSD = row.SpecRiskUSD,
                    PredBetaUSD = row.PredBetaUSD
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PivotDashboard> GetPivotDashboard(string Username)
        {
            List<PivotDashboard> ReturnValue = new List<PivotDashboard>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetPivotDashboard", null);

            foreach (dynamic row in reader)
            {
                var dash = new PivotDashboard
                {
                    Date = row.BusinessDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    SecurityType = row.SecurityType,
                    Position = row.Quantity,
                    SEMV = row.Exposure,
                    Pnl = row.DTD,
                    AnalystMultiplier = row.AnalystMultiplier,
                    //MTD = row.MTD,
                    //YTD = row.YTD,
                    PortfolioTimestamp = row.PortfolioTimestamp
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardSummary> GetPerformanceDashboardSummary(string EntityType, string Entity)
        {
            List<PerformanceDashboardSummary> ReturnValue = new List<PerformanceDashboardSummary>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@EntityType", EntityType));
            parameters.Add(new SqlParameter("@Entity", Entity));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "risk.GetPerformanceDashboardSummary", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardSummary
                {
                    EntityType = row.EntityType,
                    Entity = row.Entity,
                    FactorGroup = row.FactorGroup,
                    ITDPnl = Convert.ToDecimal(row.ITDPnl),
                    YTDPnl = Convert.ToDecimal(row.YTDPnl),
                    QTDPnl = Convert.ToDecimal(row.QTDPnl),
                    MTDPnl = Convert.ToDecimal(row.MTDPnl),
                    WTDPnl = Convert.ToDecimal(row.WTDPnl),
                    DTDPnl = Convert.ToDecimal(row.DTDPnl),
                    ITDIR = Convert.ToDecimal(row.ITDIR),
                    YTDIR = Convert.ToDecimal(row.YTDIR),
                    QTDIR = Convert.ToDecimal(row.QTDIR),
                    MTDIR = Convert.ToDecimal(row.MTDIR),
                    WTDIR = Convert.ToDecimal(row.WTDIR),
                    DTDIR = Convert.ToDecimal(row.DTDIR)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardSummaryFactorGroupPnl> GetPerformanceDashboardSummaryFactorGroupPnl(string EntityType, string Entity)
        {
            List<PerformanceDashboardSummaryFactorGroupPnl> ReturnValue = new List<PerformanceDashboardSummaryFactorGroupPnl>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@EntityType", EntityType));
            parameters.Add(new SqlParameter("@Entity", Entity));
            parameters.Add(new SqlParameter("@View", "FactorGroupPnl"));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "risk.GetPerformanceDashboardSummary", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardSummaryFactorGroupPnl
                {
                    BusDate = row.BusDate,
                    FactorGroup = row.FactorGroup,
                    Pnl = Convert.ToDecimal(row.Pnl)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardSummaryTopBottom5> GetPerformanceDashboardSummaryTopBottom5(string EntityType, string Entity)
        {
            List<PerformanceDashboardSummaryTopBottom5> ReturnValue = new List<PerformanceDashboardSummaryTopBottom5>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@EntityType", EntityType));
            parameters.Add(new SqlParameter("@Entity", Entity));
            parameters.Add(new SqlParameter("@View", "TOPBOTTOM5"));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "risk.GetPerformanceDashboardSummary", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardSummaryTopBottom5
                {
                    RowNumber = row.RowNumber,
                    EntityType = row.EntityType,
                    Direction = row.Direction,
                    EntityITD = row.EntityITD,
                    ITD = Convert.ToDecimal(row.ITD),
                    EntityYTD = row.EntityYTD,
                    YTD = Convert.ToDecimal(row.YTD),
                    EntityQTD = row.EntityQTD,
                    QTD = Convert.ToDecimal(row.QTD),
                    EntityMTD = row.EntityMTD,
                    MTD = Convert.ToDecimal(row.MTD)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardSummarySluggingRatio> GetPerformanceDashboardSummarySluggingRatio(string EntityType, string Entity)
        {
            List<PerformanceDashboardSummarySluggingRatio> ReturnValue = new List<PerformanceDashboardSummarySluggingRatio>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@EntityType", EntityType));
            parameters.Add(new SqlParameter("@Entity", Entity));
            parameters.Add(new SqlParameter("@View", "HITRATE"));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "risk.GetPerformanceDashboardSummary", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardSummarySluggingRatio
                {
                    PnlType = row.PnlType,
                    PeriodType = row.PeriodType,
                    BusDate = row.BusDate,
                    EntityType = row.EntityType,
                    Entity = row.Entity,
                    W = Convert.ToDecimal(row.W),
                    L = Convert.ToDecimal(row.L),
                    WPnl = Convert.ToDecimal(row.WPnl),
                    LPnl = Convert.ToDecimal(row.LPnl),
                    Total = Convert.ToDecimal(row.Total),
                    HitRate = Convert.ToDecimal(row.HitRate),
                    SluggingRatio = Convert.ToDecimal(row.SluggingRatio)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardIntraday> GetPerformanceDashboardSummaryIntraday()
        {
            List<PerformanceDashboardIntraday> ReturnValue = new List<PerformanceDashboardIntraday>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "risk.GetPerformanceDashboardSluggingRatioIntraday", null);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardIntraday
                {
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    PnlType = row.PnlType,
                    EntityType = row.EntityType,
                    Entity = row.Entity,

                    LongWin = Convert.ToDecimal(row.LongWin),
                    LongLoss = Convert.ToDecimal(row.LongLoss),
                    LongWinPnl = Convert.ToDecimal(row.LongWinPnl),
                    LongLossPnl = Convert.ToDecimal(row.LongLossPnl),
                    LongTotalPnl = Convert.ToDecimal(row.LongTotalPnl),
                    LongHitRate = Convert.ToDecimal(row.LongHitRate),
                    LongSluggingRatio = Convert.ToDecimal(row.LongSluggingRatio),

                    ShortWin = Convert.ToDecimal(row.ShortWin),
                    ShortLoss = Convert.ToDecimal(row.ShortLoss),
                    ShortWinPnl = Convert.ToDecimal(row.ShortWinPnl),
                    ShortLossPnl = Convert.ToDecimal(row.ShortLossPnl),
                    ShortTotalPnl = Convert.ToDecimal(row.ShortTotalPnl),
                    ShortHitRate = Convert.ToDecimal(row.ShortHitRate),
                    ShortSluggingRatio = Convert.ToDecimal(row.ShortSluggingRatio),

                    TotalWin = Convert.ToDecimal(row.TotalWin),
                    TotalLoss = Convert.ToDecimal(row.TotalLoss),
                    TotalWinPnl = Convert.ToDecimal(row.TotalWinPnl),
                    TotalLossPnl = Convert.ToDecimal(row.TotalLossPnl),
                    TotalTotalPnl = Convert.ToDecimal(row.TotalTotalPnl),
                    TotalHitRate = Convert.ToDecimal(row.TotalHitRate),
                    TotalSluggingRatio = Convert.ToDecimal(row.TotalSluggingRatio)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboardIntradayTopPosition> 
            GetPerformanceDashboardSummaryIntradayTopPositions(string EntityType, string Entity)
        {
            List<PerformanceDashboardIntradayTopPosition> ReturnValue = new List<PerformanceDashboardIntradayTopPosition>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@EntityType", EntityType));
            parameters.Add(new SqlParameter("@Entity", Entity));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "risk.GetPerformanceDashboardTopPositionsIntraday", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboardIntradayTopPosition
                {
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    PnlType = row.PnlType,
                    MetricType = row.MetricType,
                    Symbol = row.Symbol,

                    Pnl = Convert.ToDecimal(row.Pnl),
                    SEMV = Convert.ToDecimal(row.SEMV)
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PerformanceDashboard> GetPerformanceDashboard()
        {
            List<PerformanceDashboard> ReturnValue = new List<PerformanceDashboard>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "risk.GetPerformanceDashboard", null);

            foreach (dynamic row in reader)
            {
                var dash = new PerformanceDashboard
                {
                    BusDate = row.BusDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Analyst = row.Analyst,
                    FactorGroup = row.FactorGroup,
                    Factor = row.Factor,
                    FullBloombergCode = row.FullBloombergCode,
                    Pnl = Convert.ToDecimal(row.Pnl),
                    DisplayOrder = row.DisplayOrder
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<EntityCollection> GetEntities()
        {
            List<EntityCollection> ReturnValue = new List<EntityCollection>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetEntities", null);

            foreach (dynamic row in reader)
            {
                var dash = new EntityCollection
                {
                    EntityType = row.EntityType,
                    Entity = row.Entity,
                    Timestamp = row.Timestamp
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PnlAttributionDashboard> GetPnlAttributionDashboard(string Username)
        {
            List<PnlAttributionDashboard> ReturnValue = new List<PnlAttributionDashboard>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetPnlAttributionDashboard", null);

            foreach (dynamic row in reader)
            {
                var dash = new PnlAttributionDashboard
                {
                    Date = row.BusDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    SecurityDesc = row.SecurityDesc,
                    SecurityType = row.SecurityType,
                    RiskCountry = row.RiskCountry,
                    RiskCurrency = row.RiskCurrency,
                    FactorGroup = row.FactorGroup,
                    FactorDesc = row.FactorDesc,
                    Factor = row.Factor,
                    Loading = row.Loading,
                    Exposure = row.Exposure,
                    Return = row.Return,
                    Pnl = row.Pnl,
                    AnalystMultiplier = row.AnalystMultiplier
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static List<DateTime> GetIntradayTimestamps()
        {
            List<DateTime> ReturnValue = new List<DateTime>();

            IEnumerable<dynamic> reader = 
                    ExecuteInlineQueryWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "select distinct PortfolioTimestamp from core.IntradayPositionTimeSeries (nolock) order by 1");

            foreach (dynamic row in reader)
            {
                ReturnValue.Add(row.PortfolioTimestamp);
            }

            return ReturnValue;
        }

        public static IEnumerable<PnlAttribution> GetPnlAttribution(string Symbol)
        {
            List<PnlAttribution> ReturnValue = new List<PnlAttribution>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol } };
            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetPnlAttribution", parameters);

            foreach (dynamic row in reader)
            {
                var attrib = new PnlAttribution
                {
                    BusDate = row.BusDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Symbol = row.Symbol,
                    FactorGroup = row.FactorGroup,
                    Loading = row.Loading,
                    Exposure = row.Exposure,
                    Return = row.Return,
                    Pnl = row.Pnl
                };
                ReturnValue.Add(attrib);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PnlAttribution> GetPnlAttributionGrouped(string Desk, string Symbol)
        {
            List<PnlAttribution> ReturnValue = new List<PnlAttribution>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};
            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetPnlAttributionGrouped", parameters);

            foreach (dynamic row in reader)
            {
                var attrib = new PnlAttribution
                {
                    BusDate = row.BusDate,
                    FactorGroup = row.FactorGroup,
                    Loading = row.Loading,
                    Exposure = row.Exposure,
                    Return = row.Return,
                    Pnl = row.Pnl
                };
                ReturnValue.Add(attrib);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Idea> GetIdeas(string Username)
        {
            List<Idea> ReturnValue = new List<Idea>();

            List<User> analysts = GetAnalysts().ToList();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username } };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetIdeas", parameters);

            foreach (dynamic row in reader)
            {
                var dash = new Idea
                {
                    IdeaId = row.IdeaId,
                    Assignee = row.Assignee,
                    Analyst = row.Analyst,
                    Symbol = row.Symbol,
                    SecurityDesc = row.SecurityDesc,
                    IdeaStatusCode = row.IdeaStatusCode,
                    IdeaSource = row.IdeaSource,
                    ModelCompleted = row.ModelCompleted,
                    Thesis = row.Thesis,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    DaysOnList = row.DaysOnList,
                    CreatedOn = row.CreatedOn,
                    ModifiedOn = row.ModifiedOn
                };
                ReturnValue.Add(dash);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Security> GetPeerGroups(string Symbol)
        {
            List<Security> ReturnValue = new List<Security>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "sm.GetPeerGroups", parameters);

            foreach (dynamic row in reader)
            {
                var security = new Security
                {
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    PeerGroup = row.PeerGroup
                };
                ReturnValue.Add(security);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Holding> GetHoldings()
        {
            List<Holding> ReturnValue = new List<Holding>();

            string Query = "select * from Holocene.Holdings (nolock) where BusinessDate between dateadd(dd, -30, convert(date,getdate())) and convert(date,getdate())";

            IEnumerable<dynamic> reader = ExecuteInlineQueryWithResultSet(Startup.MIKDatabaseConnectionString, Query);

            foreach (dynamic row in reader)
            {
                var holding = new Holding
                {
                    BusDate = row.BusinessDate.ToString("yyyy/MM/dd"),
                    SecurityCode = row.SecurityCode,
                    FundDescription = row.FundDescription,
                    LegalEntityDescription = row.LegalEntityDescription,
                    LocationAccountDescription = row.LocationAccountDescription,
                    CustodianDescription = row.CustodianDescription,
                    Side = row.HoldingDirection,
                    QuantityStart = row.QuantityStart,
                    QuantityEnd = row.QuantityEnd,
                    StartPriceBook = row.StartPriceBook,
                    StartPriceLocal = row.StartPriceLocal,
                    EndPriceBook = row.EndPriceBook,
                    EndPriceLocal = row.EndPriceLocal,
                    StartDirectFxRate = row.StartDirectFxRate,
                    EndDirectFxRate = row.EndDirectFxRate,
                    MktValBookStart = row.MktValBookStart,
                    MktValBook = row.MktValBook,
                    MktValLocalStart = row.MktValLocalStart,
                    MktValLocal = row.MktValLocal,
                    DtdPnlTotal = row.DtdPnlTotal,
                    MtdPnlTotal = row.MtdPnlTotal,
                    YtdPnlTotal = row.YtdPnlTotal,
                    NetExposureStart = row.NetExposureStart,
                    NetExposure = row.NetExposure,
                    GrossExposureStart = row.GrossExposureStart,
                    GrossExposure = row.GrossExposure,
                    LongExposureStart = row.LongExposureStart,
                    LongExposure = row.LongExposure,
                    ShortExposureStart = row.ShortExposureStart,
                    ShortExposure = row.ShortExposure
                };
                ReturnValue.Add(holding);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetPositions(string Symbol, string Desk)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetPositions", parameters);

            foreach (dynamic row in reader)
            {
                var position = new Position
                {
                    BusDate = row.BusDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    SecurityDesc = row.SecurityDesc,
                    SecurityType = row.SecurityType,
                    IssueCountry = row.IssueCountry,
                    RiskCountry = row.RiskCountry,
                    PricingCurrency = row.PricingCurrency,
                    RiskCurrency = row.RiskCurrency,
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    PositionSOD = row.PositionSOD,
                    PositionClose = row.Position,
                    MarketValue = row.MarketValue,
                    MarketValueLocal = row.MarketValueLocal,
                    DTD = row.DTD,
                    MTD = row.MTD,
                    YTD = row.YTD,
                    NetExposureSOD = row.NetExposureSOD,
                    GrossExposureSOD = row.GrossExposureSOD,
                    LongExposureSOD = row.LongExposureSOD,
                    ShortExposureSOD = row.ShortExposureSOD,
                    NetExposure = row.NetExposure,
                    GrossExposure = row.GrossExposure,
                    LongExposure = row.LongExposure,
                    ShortExposure = row.ShortExposure,
                    PriceSOD = row.PriceSOD,
                    Price = row.Price,
                    PriceLocal = row.PriceLocal,
                    PriceSODLocal = row.PriceSODLocal,
                    Fx = row.Fx,
                    CUSIP = row.CUSIP
                };
                ReturnValue.Add(position);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetPositionsGrouped(string Symbol, string Desk)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "core.GetPositionsGrouped2", parameters);

            foreach (dynamic row in reader)
            {
                var position = new Position
                {
                    BusDate = row.BusDate,
                    Desk = row.Desk,
                    Symbol = row.Symbol,
                    DTD = row.DTD,
                    NetExposureSOD = row.NetExposureSOD,
                    GrossExposureSOD = row.GrossExposureSOD,
                    NetExposure = row.NetExposure,
                    GrossExposure = row.GrossExposure
                };
                ReturnValue.Add(position);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetIntradayPositions(string Symbol, string Desk)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@UnderlyingSymbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetCurrentPositions", parameters);

            foreach (dynamic row in reader)
            {
                var position = new Position
                {
                    BusDate = row.BusinessDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Symbol = row.SecurityCode,
                    SecurityDesc = row.SecurityDesc,
                    PortfolioTimestamp = row.ArchiveTimeStamp,
                    PositionSOD = row.SODPosition,
                    PositionClose = row.TheoreticalPosition,
                    DTD = row.DTD,
                    MTD = row.MTD,
                    YTD = row.YTD,
                    IdioPnl = row.IdioPnl,
                    IdioVol = row.IdioVol,
                    NetExposureSOD = row.NetExposureSOD,
                    NetExposure = row.NetExposure,
                    GrossExposure = row.GrossExposure,
                    LongExposure = row.LongExposure,
                    ShortExposure = row.ShortExposure
                };
                ReturnValue.Add(position);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetCurrentPositionsWithRiskLimits(string Symbol, bool IncludeOptions)
        {
            List<Position> ReturnValue = new List<Position>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@IncludeOptions", SqlDbType = SqlDbType.Bit, Value = IncludeOptions }
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetCurrentPositionsWithRiskLimits", parameters);

            foreach (dynamic row in reader)
            {
                var position = new Position
                {
                    BusDate = row.BusDate,
                    Desk = row.Desk,
                    Side = row.Side,
                    Symbol = row.Symbol,
                    UnderlyingSymbol = row.UnderlyingSymbol,
                    PortfolioTimestamp = row.PortfolioTimestamp,
                    DTD = row.DTD,
                    IdioPnl = row.IdioPnl,
                    IdioVol = row.IdioVol,
                    NetExposureSOD = row.SEMVSOD,
                    NetExposure = row.SEMV,
                    RiskLimit = row.RiskLimit,
                    TheoNetExposure = Convert.ToDecimal(row.TheoSEMV)
                };
                ReturnValue.Add(position);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Position> GetIntradayPositionsEze()
        {
            List<Position> ReturnValue = new List<Position>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "eze.GetIntradayPositions", null);

            foreach (dynamic row in reader)
            {
                var position = new Position
                {
                    BusDate = row.BusinessDate,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    PositionBlock = row.PositionBlock,
                    Side = row.Side,
                    Symbol = row.SecurityCode,
                    SecurityDesc = row.SecurityDesc,
                    PortfolioTimestamp = row.ArchiveTimeStamp,
                    PositionSOD = row.SODPosition,
                    PositionClose = row.TheoreticalPosition,
                    DTD = row.DTD,
                    MTD = row.MTD,
                    YTD = row.YTD,
                    IdioPnl = row.IdioPnl,
                    IdioVol = row.IdioVol,
                    NetExposureSOD = row.NetExposureSOD,
                    NetExposure = row.NetExposure,
                    GrossExposure = row.GrossExposure,
                    LongExposure = row.LongExposure,
                    ShortExposure = row.ShortExposure
                };
                ReturnValue.Add(position);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<UserCustomSetting> GetUserCustomSettings(string FieldGroup)
        {
            List<UserCustomSetting> ReturnValue = new List<UserCustomSetting>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@FieldGroup", SqlDbType = SqlDbType.NVarChar, Value = FieldGroup }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "app.GetUserCustomSettings", parameters);

            foreach (dynamic row in reader)
            {
                var setting = new UserCustomSetting
                {
                    UserId = row.UserId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    FieldGroup = row.FieldGroup,
                    FieldName = row.FieldName,
                    FieldValue1 = row.FieldValue1,
                    FieldValue2 = row.FieldValue2,
                    FieldValue3 = row.FieldValue3,
                    ModifiedOn = Convert.ToDateTime(row.ModifiedOn)
                };
                ReturnValue.Add(setting);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<UserCustomSetting> GetUserCustomSetting(string FieldGroup, string FieldName, string FieldValue1, string FieldValue2, string FieldValue3)
        {
            List<UserCustomSetting> ReturnValue = new List<UserCustomSetting>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                new SqlParameter() {ParameterName = "@FieldGroup", SqlDbType = SqlDbType.NVarChar, Value = FieldGroup },
                new SqlParameter() {ParameterName = "@FieldName", SqlDbType = SqlDbType.NVarChar, Value = FieldName },
                new SqlParameter() {ParameterName = "@FieldValue1", SqlDbType = SqlDbType.NVarChar, Value = FieldValue1 },
                new SqlParameter() {ParameterName = "@FieldValue2", SqlDbType = SqlDbType.NVarChar, Value = FieldValue2 },
                new SqlParameter() {ParameterName = "@FieldValue3", SqlDbType = SqlDbType.NVarChar, Value = FieldValue3 }
            };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "app.GetUserCustomSettings", parameters);

            foreach (dynamic row in reader)
            {
                var setting = new UserCustomSetting
                {
                    UserId = row.UserId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    FieldGroup = row.FieldGroup,
                    FieldName = row.FieldName,
                    FieldValue1 = row.FieldValue1,
                    FieldValue2 = row.FieldValue2,
                    FieldValue3 = row.FieldValue3,
                    ModifiedOn = Convert.ToDateTime(row.ModifiedOn)
                };
                ReturnValue.Add(setting);
            }

            return ReturnValue.AsEnumerable();
        }

        public static int UpsertUserCustomSetting(string Username, string FieldGroup, string FieldName, string FieldValue1, 
            string FieldValue2, string FieldValue3)
        {
            int ReturnValue;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@FieldGroup", SqlDbType = SqlDbType.NVarChar, Value = FieldGroup },
                    new SqlParameter() {ParameterName = "@FieldName", SqlDbType = SqlDbType.NVarChar, Value = FieldName },
                    new SqlParameter() { ParameterName = "@FieldValue1", SqlDbType = SqlDbType.NVarChar, Value = FieldValue1 },
                    new SqlParameter() { ParameterName = "@FieldValue2", SqlDbType = SqlDbType.NVarChar, Value = FieldValue2 },
                    new SqlParameter() { ParameterName = "@FieldValue3", SqlDbType = SqlDbType.NVarChar, Value = FieldValue3 }
                };

            ReturnValue = ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString,
                "app.UpsertUserCustomSetting", parameters).Value;

            return ReturnValue;
        }

        public static IEnumerable<AlphaReturn> GetAlphaReturns(string Desk)
        {
            List<AlphaReturn> ReturnValue = new List<AlphaReturn>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "report.GetResidualReturns", parameters);

            foreach (dynamic row in reader)
            {
                var resreturn = new AlphaReturn
                {
                    BarraMostRecentDate = row.BarraMostRecentDate,
                    Symbol = row.Symbol,
                    Cusip = row.Cusip,
                    BarraId = row.BarraId,
                    CurrentSEMV = row.CurrentSEMV,
                    IdioVol = row.IdioVol,
                    RES_1W = row.RES_1W,
                    RES_1M = row.RES_1M,
                    RES_3M = row.RES_3M,
                    RES_6M = row.RES_6M,
                    RES_1Y = row.RES_1Y
                };
                ReturnValue.Add(resreturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<BarraCache> GetBarraCache()
        {
            List<BarraCache> ReturnValue = new List<BarraCache>();

            IEnumerable<dynamic> reader = ExecuteInlineQueryWithResultSet(Startup.HoloceneDatabaseConnectionString,
                    "select * from barra.Cache (nolock)");

            foreach (dynamic row in reader)
            {
                var resreturn = new BarraCache
                {
                    SourceUpdatedOn = row.SourceUpdatedOn,
                    Symbol = row.Symbol,
                    Cusip = row.Cusip,
                    BarraId = row.BarraId,
                    IdioVol = row.IdioVol,
                    IdioReturn = row.IdioReturn,
                    PredBeta = row.PredBeta,
                    RES_1W = row.RES_1W,
                    RES_2W = row.RES_2W,
                    RES_3W = row.RES_3W,
                    RES_1M = row.RES_1M,
                    RES_3M = row.RES_3M,
                    RES_6M = row.RES_6M,
                    RES_9M = row.RES_9M,
                    RES_1Y = row.RES_1Y,
                    RES_YTD = row.RES_YTD
                };
                ReturnValue.Add(resreturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<RiskLimit> GetDailyRiskLimits(string Symbol)
        {
            List<RiskLimit> ReturnValue = new List<RiskLimit>();

            List<SqlParameter> parameters = null;
            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar,
                        Value = Symbol }};
            }

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetDailyRiskLimits", parameters);

            foreach (dynamic row in reader)
            {
                var limit = new RiskLimit
                {
                    Desk = row.Desk,
                    Symbol = row.Symbol,
                    Limit = Convert.ToDecimal(row.Limit)
                };
                ReturnValue.Add(limit);
            }

            return ReturnValue.AsEnumerable();
        }

        private static IEnumerable<dynamic> PostgresExecuteStoredProcedureWithResultSet(string DatabaseConnectionString, string RawSql)
        {
            using (var connection = new Npgsql.NpgsqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = new Npgsql.NpgsqlCommand(RawSql, connection);

                using (var dataReader = command.ExecuteReader())
                {
                    var fields = new List<String>();

                    for (var i = 0; i < dataReader.FieldCount; i++)
                    {
                        fields.Add(dataReader.GetName(i));
                    }

                    while (dataReader.Read())
                    {
                        var item = new ExpandoObject() as IDictionary<String, Object>;

                        for (var i = 0; i < fields.Count; i++)
                        {
                            item.Add(fields[i], dataReader[fields[i]]);
                        }

                        yield return item;
                    }
                }
            }
        }

        private static object PostgresExecuteStoredProcedureWithReturnValue(string DatabaseConnectionString, string RawSql)
        {
            object ReturnValue;
            using (var connection = new Npgsql.NpgsqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = new Npgsql.NpgsqlCommand(RawSql, connection);
                ReturnValue = command.ExecuteScalar();
            }

            return ReturnValue;
        }

        private static bool PostgresExecuteStoredProcedure(string DatabaseConnectionString, string RawSql)
        {
            using (var connection = new Npgsql.NpgsqlConnection(DatabaseConnectionString))
            {
                connection.Open();

                var command = new Npgsql.NpgsqlCommand(RawSql, connection);
                command.ExecuteNonQuery();
            }

            return true;
        }

        public static IEnumerable<CoverageUniverse> GetCoverageUniverse(string Desk)
        {
            List<CoverageUniverse> ReturnValue = new List<CoverageUniverse>();

            Desk = (string.IsNullOrEmpty(Desk) ? string.Empty : Desk);
            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Desk", SqlDbType = SqlDbType.NVarChar, Value = Desk }};

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "report.GetCoverageUniverse", parameters);

            foreach (dynamic row in reader)
            {
                var universe = new CoverageUniverse
                {
                    Symbol = row.Symbol,
                    BarraId = row.BarraId,
                    Desk = row.Desk,
                    SEMV = Convert.ToDecimal(row.SEMV),
                    TheoSEMV = Convert.ToDecimal(row.TargetSEMV),
                    DTD = Convert.ToDecimal(row.DTD)
                };
                ReturnValue.Add(universe);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Tweet> GetTweets(string Username, int TweetId)
        {
            List<Tweet> ReturnValue = new List<Tweet>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@TweetId", SqlDbType = SqlDbType.Int, Value = TweetId }};

            IEnumerable<dynamic> reader = DatabaseHelper
                .ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "core.GetTweets", parameters);

            foreach (dynamic row in reader)
            {
                var tweet = new Tweet
                {
                    TweetId = row.TweetId,
                    Timestamp = row.Timestamp,
                    Message = row.Tweet,
                    Hashtags = row.Hashtags,
                    Username = row.Username,
                    UserDisplayName = row.UserDisplayName,
                    EmailAddress = row.EmailAddress,
                    ExpirationDate = Convert.ToDateTime(row.ExpirationDate)
                };
                ReturnValue.Add(tweet);
            }

            return ReturnValue.AsEnumerable();
        }

        public static int InsertTweet(string Username, DateTime Timestamp, string Tweet, string Hashtags)
        {
            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@Tweet", SqlDbType = SqlDbType.NVarChar, Value = Tweet },
                    new SqlParameter() {ParameterName = "@Hashtags", SqlDbType = SqlDbType.NVarChar, Value = Hashtags },
                    new SqlParameter() { ParameterName = "@Timestamp", SqlDbType = SqlDbType.DateTime, Value = Timestamp }
                };

            return ExecuteStoredProcedureWithReturnValue(Startup.HoloceneDatabaseConnectionString, "core.InsertTweet", parameters).Value;
        }

        public static int UpsertTweet(int TweetId,
            string Username, DateTime Timestamp, string Tweet, string Hashtags)
        {
            int ReturnValue = 0;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@TweetId", SqlDbType = SqlDbType.Int, Value = TweetId },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@Tweet", SqlDbType = SqlDbType.NVarChar, Value = Tweet },
                    new SqlParameter() {ParameterName = "@Hashtags", SqlDbType = SqlDbType.NVarChar, Value = Hashtags },
                    new SqlParameter() { ParameterName = "@Timestamp", SqlDbType = SqlDbType.DateTime, Value = Timestamp }
                };

            ExecuteStoredProcedure(
                Startup.HoloceneDatabaseConnectionString, "core.UpsertTweet", parameters);

            return ReturnValue;
        }

        public static IEnumerable<FactorLoading> GetFactorLoadings(string BarraId)
        {
            List<FactorLoading> ReturnValue = new List<FactorLoading>();

            string RawSql = "select a.barraid,a.date,a.factor,coalesce(b.loading,a.loading) loading" +
                " from qr.public.usmeds_asset_exp_last a left join public.usmeds_asset_exp_override b on" +
                " (a.barraid = b.barraid and a.factor = b.factor and a.date between b.start_date and b.end_date)" +
                " where a.barraid = '" + BarraId + "'";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var loading = new FactorLoading
                {
                    BusDate = row.date,
                    BarraId = row.barraid,
                    FactorName = row.factor,
                    Loading = (row.loading is System.DBNull ? null : Convert.ToDecimal(row.loading))
                };

                ReturnValue.Add(loading);
            }

            if (ReturnValue.Count() == 0)
            {
                RawSql = $@"
                        select 
                            barraid,'{DateTime.Now.ToString("MM/dd/yyyy")}' date,factor,loading
                        from 
                            qr.public.usmeds_asset_exp_override
                        where 
                            barraid = '{BarraId}' and
                            '{DateTime.Now.ToString("MM/dd/yyyy")}' between start_date and end_date";

                var readerOverride = PostgresExecuteStoredProcedureWithResultSet
                    (Startup.QRDatabaseConnectionString, RawSql);

                foreach (dynamic row in readerOverride)
                {
                    var loading = new FactorLoading
                    {
                        BusDate = Convert.ToDateTime(row.date),
                        BarraId = row.barraid,
                        FactorName = row.factor,
                        Loading = (row.loading is System.DBNull ? null : Convert.ToDecimal(row.loading))
                    };

                    ReturnValue.Add(loading);
                }
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<FactorLoading> GetCrowdingLoadings(string Symbol)
        {
            List<FactorLoading> ReturnValue = new List<FactorLoading>();

            string RawSql = "select a.symbol,'' barraid,a.rpt_dt date,b.factor,b.loading" +
                    " from qr.crowding.secinfo a" +
                    " join qr.crowding.loadings b on (a.cusip = b.cusip and a.rpt_dt = b.rpt_dt)" +
                    " where b.factor = 'OWNL' and a.symbol = '" + Symbol + "' and" +
                    " a.rpt_dt = (select max(rpt_dt) from qr.crowding.secinfo where symbol = '" + Symbol + "')";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var loading = new FactorLoading
                {
                    Symbol = row.symbol,
                    BusDate = row.date,
                    BarraId = row.barraid,
                    FactorName = row.factor,
                    Loading = (row.loading is System.DBNull ? null : Convert.ToDecimal(row.loading))
                };

                ReturnValue.Add(loading);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<FactorLoading> GetFactorLoadingsHistory(string Factors, string BarraIds, DateTime StartDate)
        {
            List<FactorLoading> ReturnValue = new List<FactorLoading>();

            string RawSql = "select barraid,date,factor,loading from qr.public.usmeds_asset_exp where " +
                    " date >= '" + StartDate.ToString("yyyy-MM-dd") + "' and" +
                    " factor in (" + Factors + ") and" +
                    " barraid in (" + BarraIds + ")";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var loading = new FactorLoading
                {
                    BusDate = row.date,
                    BarraId = row.barraid,
                    FactorName = row.factor,
                    Loading = (row.loading is System.DBNull ? null : Convert.ToDecimal(row.loading))
                };

                ReturnValue.Add(loading);
            }

            return ReturnValue.AsEnumerable();
        }     

        public static IEnumerable<FactorReturn> GetFactorReturnsHistory(DateTime StartDate)
        {
            List<FactorReturn> ReturnValue = new List<FactorReturn>();

            string RawSql = "select factor,dlyreturn,date from qr.public.usmeds_fac_ret" +
                    " where date >= '" + StartDate.ToString("yyyy-MM-dd") + "'";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var facReturn = new FactorReturn
                {
                    Timestamp = Convert.ToDateTime(row.date),
                    Name = row.factor,
                    Return = Convert.ToDecimal(row.dlyreturn)
                };

                ReturnValue.Add(facReturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<RiskReturn> GetFactorGroupReturnsHistory(DateTime StartDate, string BarraIds, string Factors)
        {
            List<RiskReturn> ReturnValue = new List<RiskReturn>();

            string RawSql = $@"
                    select 
                        a.barraid,a.date,coalesce(sum(coalesce(c.loading,a.loading) * b.dlyreturn),0) totalreturn 
                    from 
                        qr.public.usmeds_asset_exp a
                        left join public.usmeds_asset_exp_override c on (a.barraid = c.barraid and a.factor = c.factor and a.date between c.start_date and c.end_date)
                        join qr.public.usmeds_fac_ret b on (a.factor = b.factor and a.date = b.date)
                    where
                        a.date >= '{StartDate.ToString("yyyy-MM-dd")}' and 
                        a.factor in ({Factors}) and
                        a.barraid = {BarraIds}
                    group by a.barraid,a.date 
                    order by a.barraid,a.date";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var facReturn = new RiskReturn
                {
                    BusDate = Convert.ToDateTime(row.date),
                    BarraId = row.barraid,
                    Return = Convert.ToDecimal(row.totalreturn)
                };

                ReturnValue.Add(facReturn);
            }

            RawSql = $@"
                    select a.barraid,b.date,coalesce(sum(a.loading*b.dlyreturn),0) totalreturn 
                    from 
                        qr.public.usmeds_asset_exp_override a 
                        join qr.public.usmeds_fac_ret b on (a.factor = b.factor and b.date between a.start_date and a.end_date)  
                    where  
                        b.date >= '{StartDate.ToString("yyyy-MM-dd")}' and 
                        a.factor in ({Factors}) and 
                        a.barraid = {BarraIds}
                    group by a.barraid,b.date
                    order by a.barraid,b.date
                    ";            

            var readerOverride = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in readerOverride)
            {
                var facReturnOverride = new RiskReturn
                {
                    BusDate = Convert.ToDateTime(row.date),
                    BarraId = row.barraid,
                    Return = Convert.ToDecimal(row.totalreturn)
                };

                if (ReturnValue.Where(x => x.BusDate == Convert.ToDateTime(row.date)).Count() == 0)
                {
                    ReturnValue.Add(facReturnOverride);                                        
                }                
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<RiskReturn> GetFactorGroupPlusSpecificReturnsHistory(DateTime StartDate, string BarraIds, string Factors)
        {
            List<RiskReturn> ReturnValue = new List<RiskReturn>();

            string RawSql = "select a.barraid,a.date,coalesce(sum(a.loading*b.dlyreturn),0) + coalesce(max(c.specific_return)/100,0) totalreturn from qr.public.usmeds_asset_exp a " +
                    " join qr.public.usmeds_fac_ret b on (a.factor = b.factor and a.date = b.date) " +
                    " left join qr.public.usmed_specific_return c on (a.barraid = c.barraid and a.date = c.date) " +
                    " where a.date >= '" + StartDate.ToString("yyyy-MM-dd") + "' and" +
                    " a.factor in (" + Factors + ") and" +
                    " a.barraid = " + BarraIds + "" +
                    " group by a.barraid,a.date";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var facReturn = new RiskReturn
                {
                    BusDate = Convert.ToDateTime(row.date),
                    BarraId = row.barraid,
                    Return = Convert.ToDecimal(row.totalreturn)
                };

                ReturnValue.Add(facReturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<RiskReturn> GetSpecificReturnHistory(string BarraIds, DateTime StartDate)
        {
            List<RiskReturn> ReturnValue = new List<RiskReturn>();

            string RawSql = "select barraid,specific_return/100 specific_return,date from qr.public.usmed_specific_return" +
                    " where date >= '" + StartDate.ToString("yyyy-MM-dd") + "' and" +
                    " barraid in (" + BarraIds + ")";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var specReturn = new RiskReturn
                {
                    BusDate = Convert.ToDateTime(row.date),
                    BarraId = row.barraid,
                    Return = Convert.ToDecimal(row.specific_return)
                };

                ReturnValue.Add(specReturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<FactorReturn> GetFactorReturns()
        {
            List<FactorReturn> ReturnValue = new List<FactorReturn>();

            string RawSql = "select ts,factor,ret ret from mkt_data.factor_returns";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                TimeSpan ts = row.ts;
                string ts1 = ts.ToString(@"hh\:mm\:ss");
                var facReturn = new FactorReturn
                {
                    Timestamp = Convert.ToDateTime(DateTime.Now.ToString("yyyy/MM/dd") + " " + ts1),
                    Name = row.factor,
                    Return = Convert.ToDecimal(row.ret)
                };

                ReturnValue.Add(facReturn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<FactorVol> GetFactorVol()
        {
            List<FactorVol> ReturnValue = new List<FactorVol>();

            string RawSql = "select factor1 factor,sqrt(covar)/(100*sqrt(252)) vol from public.usmeds_fac_cov_last where factor1 = factor2";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var facVol = new FactorVol
                {
                    Name = row.factor,
                    Vol = Convert.ToDecimal(row.vol)
                };

                ReturnValue.Add(facVol);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Factor> GetFactors()
        {
            List<Factor> ReturnValue = new List<Factor>();

            string RawSql = @"select b.FactorGroup,b.BarraFactorGroup,a.Factor FactorName,a.FactorDisplayName FactorDesc
            from barra.Factor a join risk.FactorGroupMap(nolock) b on(upper(a.FactorGroup) = upper(b.BarraFactorGroup))";
            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var factor = new Factor
                {
                    BarraFactorGroup = row.BarraFactorGroup,
                    FactorGroup = row.FactorGroup,
                    FactorName = row.FactorName,
                    FactorDesc = row.FactorDesc
                };

                ReturnValue.Add(factor);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<FactorGroup> GetFactorGroups()
        {
            List<FactorGroup> ReturnValue = new List<FactorGroup>();

            string RawSql = "select FactorGroup,BarraFactorGroup from risk.FactorGroupMap (nolock)";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var factor = new FactorGroup
                {
                    BarraFactorGroup = row.BarraFactorGroup,
                    FactorGroupDesc = row.FactorGroup
                };

                ReturnValue.Add(factor);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<Trade> GetTrades()
        {
            List<Trade> ReturnValue = new List<Trade>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetTrades", null);

            foreach (dynamic row in reader)
            {
                var trade = new Trade
                {
                    TradeDate = row.TradeDate,
                    TradeTime = row.ExecutionTime,
                    SecurityCode = row.SecurityCode,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    ExecutionBroker = row.ExecutionBroker,
                    Strategy = row.Strategy,
                    Desk = row.Desk,
                    Quantity = row.Quantity,
                    NetMoney = row.NetMoney
                };
                ReturnValue.Add(trade);
            }

            return ReturnValue.AsEnumerable();
        }

        public static ResearchEvent GetResearchEvent(int ResearchEventId)
        {
            ResearchEvent ReturnValue = new ResearchEvent();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@ResearchEventId", SqlDbType = SqlDbType.Int, Value = ResearchEventId } };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "research.GetResearchEvents", parameters);

            foreach (dynamic row in reader)
            {
                ReturnValue = new ResearchEvent
                {
                    EventDate = row.EventDate,
                    EventType = row.EventType,
                    Author = row.Author,
                    Company = row.Company,
                    Contact = row.Contact,
                    ContactType = row.ContactType,
                    ExternalReferenceId = row.ExternalReferenceId,
                    Symbol = row.Symbol,
                    Title = row.Title,
                    Tone = row.Tone,
                    HtmlContent = row.HtmlString,
                    ModifiedOn = row.ModifiedOn,
                    SentimentScore = row.SentimentScore,
                    AttachmentName1 = row.AttachmentName1 is DBNull ? null : row.AttachmentName1,
                    AttachmentName2 = row.AttachmentName2 is DBNull ? null : row.AttachmentName2
                };
            }

            return ReturnValue;
        }

        public static List<ResearchEvent> GetResearchEvents(string Symbol)
        {
            List<ResearchEvent> ReturnValue = new List<ResearchEvent>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol } };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "research.GetResearchEvents", parameters);

            foreach (dynamic row in reader)
            {
                var researchEvent = new ResearchEvent
                {
                    ResearchEventId = row.ResearchEventId,
                    EventDate = row.EventDate,
                    EventType = row.EventType,
                    Author = row.Author,
                    Company = row.Company,
                    Contact = row.Contact,
                    ContactType = row.ContactType,
                    ExternalReferenceId = row.ExternalReferenceId,
                    Symbol = row.Symbol,
                    Title = row.Title,
                    Tone = row.Tone,
                    ModifiedOn = row.ModifiedOn,
                    SentimentScore = row.SentimentScore,
                    AttachmentName1 = row.AttachmentName1 is DBNull ? null : row.AttachmentName1,
                    AttachmentName2 = row.AttachmentName2 is DBNull ? null : row.AttachmentName2
                };
                ReturnValue.Add(researchEvent);
            }

            return ReturnValue;
        }

        public static List<ResearchEvent> GetResearchEvents(string Symbol, DateTime BusDate)
        {
            List<ResearchEvent> ReturnValue = new List<ResearchEvent>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@BusDate", SqlDbType = SqlDbType.NVarChar, Value = BusDate.ToString("MM/dd/yyyy") },
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "research.GetResearchEvents", parameters);

            foreach (dynamic row in reader)
            {
                var researchEvent = new ResearchEvent
                {
                    ResearchEventId = row.ResearchEventId,
                    EventDate = row.EventDate,
                    EventType = row.EventType,
                    Author = row.Author,
                    Company = row.Company,
                    Contact = row.Contact,
                    ContactType = row.ContactType,
                    ExternalReferenceId = row.ExternalReferenceId,
                    Symbol = row.Symbol,
                    Title = row.Title,
                    Tone = row.Tone,
                    HtmlContent = row.HtmlString,
                    ModifiedOn = row.ModifiedOn,
                    SentimentScore = row.SentimentScore,
                    AttachmentName1 = row.AttachmentName1 is DBNull ? null : row.AttachmentName1,
                    AttachmentName2 = row.AttachmentName2 is DBNull ? null : row.AttachmentName2
                };
                ReturnValue.Add(researchEvent);
            }

            return ReturnValue;
        }

        public static List<AnalystIdea> GetAnalystIdeas(string Username, string SEMVType)
        {
            List<AnalystIdea> ReturnValue = new List<AnalystIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@TMTSEMVAggregation", SqlDbType = SqlDbType.NVarChar, Value = SEMVType }
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetAnalystIdeas", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new AnalystIdea
                {
                    UserId = row.UserId,
                    SecurityId = row.SecurityId,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    ExpectedValueId = (row.ExpectedValueId is System.DBNull ? null : row.ExpectedValueId),
                    EV = (row.EV is System.DBNull ? null : row.EV),
                    ER = (row.ER is System.DBNull ? null : row.ER),
                    EVAge = (row.EVAge is System.DBNull ? null : row.EVAge),
                    IdeaAge = (row.IdeaAge is System.DBNull ? null : row.IdeaAge),
                    IsPriceLive = false,
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    Direction = row.Direction,
                    DirectionDesc = row.DirectionDesc,
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    IdeaAbstractLongTermViewCode = row.IdeaAbstractLongTermViewCode,
                    IdeaAbstractLongTermViewDesc = row.IdeaAbstractLongTermViewDesc,
                    IdeaAbstractShortTermThesis = row.IdeaAbstractShortTermThesis,
                    IdeaAbstractLongTermThesis = row.IdeaAbstractLongTermThesis,
                    IsHotIdea = Convert.ToBoolean(row.IsHotIdea),
                    HotIdeaAge = (row.HotIdeaAge is System.DBNull ? null : row.HotIdeaAge),
                    IsOppositeSide = row.IsOppositeSide,
                    PortfolioTimestamp = (row.PortfolioTimestamp is System.DBNull ? null : row.PortfolioTimestamp),
                    SEMV = (row.SEMV is System.DBNull ? null : row.SEMV),
                    SEMVFund = (row.SEMVFund is System.DBNull ? null : row.SEMVFund),
                    PreviousIdeaAbstractShortTermViewCode = row.PreviousIdeaAbstractShortTermViewCode,
                    PreviousIdeaAbstractShortTermViewDesc = row.PreviousIdeaAbstractShortTermViewDesc,
                    DollarIdio = (row.DollarIdio is System.DBNull ? null : row.DollarIdio),
                    DollarIdioFund = (row.DollarIdioFund is System.DBNull ? null : row.DollarIdioFund),
                    EarningsThesisFilename = row.EarningsFilename,
                    ThesisId = row.ThesisId,
                    ThesisCreatedOn = row.ThesisCreatedOn is DBNull ? null : row.ThesisCreatedOn,
                    ThesisText = row.ThesisText
                };
                ReturnValue.Add(rec);
            }

            return ReturnValue;
        }

        public static List<AnalystIdea> GetAnalystIdeaAlerts(string Username, string Entity,
            bool IncludeSelf)
        {
            List<AnalystIdea> ReturnValue = new List<AnalystIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@Entity", SqlDbType = SqlDbType.NVarChar, Value = Entity },
                    new SqlParameter() {ParameterName = "@IncludeSelf", SqlDbType = SqlDbType.Bit, Value = IncludeSelf }
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "report.GetAnalystIdeasAlerts", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new AnalystIdea
                {
                    AsOfTimestamp = Convert.ToDateTime(row.AsOfTimestamp),
                    UserId = row.UserId,
                    SecurityId = row.SecurityId,
                    Desk = row.Desk,
                    Sector = row.Sector,
                    ExpectedValueId = (row.ExpectedValueId is System.DBNull ? null : row.ExpectedValueId),
                    EV = (row.EV is System.DBNull ? null : row.EV),
                    ER = (row.ER is System.DBNull ? null : row.ER),
                    EVAge = (row.EVAge is System.DBNull ? null : row.EVAge),
                    IdeaAge = (row.IdeaAge is System.DBNull ? null : row.IdeaAge),
                    IsPriceLive = false,
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    DirectionDesc = row.DirectionDesc,
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    IdeaAbstractLongTermViewCode = row.IdeaAbstractLongTermViewCode,
                    IdeaAbstractLongTermViewDesc = row.IdeaAbstractLongTermViewDesc,
                    IdeaAbstractShortTermThesis = row.IdeaAbstractShortTermThesis,
                    IdeaAbstractLongTermThesis = row.IdeaAbstractLongTermThesis,
                    IsHotIdea = Convert.ToBoolean(row.IsHotIdea),
                    HotIdeaAge = (row.HotIdeaAge is System.DBNull ? null : row.HotIdeaAge),
                    IsOppositeSide = row.IsOppositeSide,
                    IdeaConflict = Convert.ToBoolean(row.IdeaConflict),
                    PortfolioTimestamp = (row.PortfolioTimestamp is System.DBNull ? null : row.PortfolioTimestamp),
                    SEMV = (row.SEMV is System.DBNull ? null : row.SEMV),
                    PreviousIdeaAbstractShortTermViewCode = row.PreviousIdeaAbstractShortTermViewCode,
                    PreviousIdeaAbstractShortTermViewDesc = row.PreviousIdeaAbstractShortTermViewDesc,
                    DollarIdio = (row.DollarIdio is System.DBNull ? null : row.DollarIdio)
                };
                ReturnValue.Add(rec);
            }

            return ReturnValue;
        }

        public static List<AnalystIdea> GetUpcomingEarnings(string Username, string SEMVType)
        {
            List<AnalystIdea> ReturnValue = new List<AnalystIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@TMTSEMVAggregation", SqlDbType = SqlDbType.NVarChar, Value = SEMVType }

                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetUpcomingEarnings", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new AnalystIdea
                {
                    UserId = row.UserId,
                    SecurityId = row.SecurityId,
                    Desk = row.Desk,
                    ExpectedValueId = (row.ExpectedValueId is System.DBNull ? null : row.ExpectedValueId),
                    EV = (row.EV is System.DBNull ? null : row.EV),
                    ER = (row.ER is System.DBNull ? null : row.ER),
                    EVAge = (row.EVAge is System.DBNull ? null : row.EVAge),
                    IdeaAge = (row.IdeaAge is System.DBNull ? null : row.IdeaAge),
                    IsPriceLive = false,
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    IdeaAbstractLongTermViewCode = row.IdeaAbstractLongTermViewCode,
                    IdeaAbstractLongTermViewDesc = row.IdeaAbstractLongTermViewDesc,
                    IdeaAbstractShortTermThesis = row.IdeaAbstractShortTermThesis,
                    IdeaAbstractLongTermThesis = row.IdeaAbstractLongTermThesis,
                    IsHotIdea = Convert.ToBoolean(row.IsHotIdea),
                    HotIdeaAge = (row.HotIdeaAge is System.DBNull ? null : row.HotIdeaAge),
                    IsOppositeSide = row.IsOppositeSide,
                    PortfolioTimestamp = (row.PortfolioTimestamp is System.DBNull ? null : row.PortfolioTimestamp),
                    SEMV = (row.SEMV is System.DBNull ? null : row.SEMV),
                    SEMVFund = (row.SEMVFund is System.DBNull ? null : row.SEMVFund),
                    DollarIdio = (row.DollarIdio is System.DBNull ? null : row.DollarIdio),
                    DollarIdioFund = (row.DollarIdioFund is System.DBNull ? null : row.DollarIdioFund),
                    EarningsThesisFilename = row.EarningsFilename,
                    IsSnoozed = Convert.ToBoolean(row.IsSnoozed)
                };
                ReturnValue.Add(rec);
            }

            return ReturnValue;
        }

        public static IEnumerable<Trade> GetTrades(string Symbol)
        {
            List<Trade> ReturnValue = new List<Trade>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString, "core.GetTrades", parameters);

            foreach (dynamic row in reader)
            {
                var trade = new Trade
                {
                    TradeDate = row.TradeDate,
                    TradeTime = row.ExecutionTime,
                    SecurityCode = row.SecurityCode,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    ExecutionBroker = row.ExecutionBroker,
                    Strategy = row.Strategy,
                    Desk = row.Desk,
                    Quantity = row.Quantity,
                    ExecutionFxRate = row.ExecutionFxRate,
                    NetMoney = row.NetMoney
                };
                ReturnValue.Add(trade);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<EarningsDetails> GetEarningsDetails(string Symbol, string Username)
        {
            List<EarningsDetails> ReturnValue = new List<EarningsDetails>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username }};

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString, "research.GetEarningsDetails", parameters);

            foreach (dynamic row in reader)
            {
                var earningsDetails = new EarningsDetails
                {
                    Desk = row.Desk,
                    Symbol = row.Symbol,
                    IsDefaultDesk = Convert.ToBoolean(row.IsDefaultDesk),
                    IdioVol = row.IdioVol,
                    IdioVolPct = row.IdioVolPct,
                    IdioVolPctDesk = row.IdioVolPctDesk,
                    IdioVolPctFirm = row.IdioVolPctFirm,
                    SEMV = row.SEMV,
                    GMVPctDesk = row.GMVPctDesk,
                    GMVPctFirm = row.GMVPctFirm,
                    ExpectedValueId = row.ExpectedValueId
                };
                ReturnValue.Add(earningsDetails);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<RiskReturn> GetIdioReturns(string Symbol, DateTime StartDate, DateTime EndDate)
        {
            List<RiskReturn> ReturnValue = new List<RiskReturn>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            parameters.Add(new SqlParameter("@StartDate", StartDate));
            parameters.Add(new SqlParameter("@EndDate", EndDate));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "risk.GetIdioReturns", parameters);

            foreach (dynamic row in reader)
            {
                var idioReturn = new RiskReturn
                {
                    Symbol = Symbol,
                    BusDate = row.Date,
                    BarraId = row.BarraId,
                    SpecificReturn = row.SpecificReturn,
                    CumulativeSpecificReturn = row.CumulativeSpecificReturn
                };
                ReturnValue.Add(idioReturn);
            }

            return ReturnValue.AsEnumerable();
        }


        public static IEnumerable<RiskReturn> GetIdioReturnsWithOverrides(string Symbol, string BarraId, DateTime StartDate)
        {
            List<RiskReturn> ReturnValue = new List<RiskReturn>();

            string RawSql = "select * from qr.public.getspecificreturns_portal('" + Symbol + "','" +
                    BarraId + "','" + StartDate.ToString("yyyy-MM-dd") + "')";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var rtn = new RiskReturn
                {
                    Symbol = row.symbol,
                    BusDate = row.busdate,
                    BarraId = row.barraid,
                    SpecificReturn = row.specreturn,
                    CumulativeSpecificReturn = row.cumulativespecreturn
                };

                ReturnValue.Add(rtn);
            }

            return ReturnValue.AsEnumerable();
        }

        public static string GetBarraId(string Symbol)
        {
            string ReturnValue = string.Empty;

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "risk.GetBarraId", parameters);

            foreach (dynamic row in reader)
            {
                ReturnValue = row.BarraId;
            }

            return ReturnValue;
        }

        public static IEnumerable<BarraSecurityRisk> GetBarraSecurityRiskData(string BarraIds, DateTime StartDate, DateTime EndDate)
        {
            List<BarraSecurityRisk> ReturnValue = new List<BarraSecurityRisk>();            

            return ReturnValue.AsEnumerable();
        }

        public static int InsertHotIdea(HotIdea Idea)
        {
            int ReturnValue = 0;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Idea.Symbol },
                    new SqlParameter() {ParameterName = "@IdeaAbstractShortTermViewCode", SqlDbType = SqlDbType.NVarChar, Value = Idea.IdeaAbstractShortTermViewCode },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Idea.Username }
                };

            ReturnValue = (int)ExecuteStoredProcedureWithReturnValue(
                Startup.HoloceneDatabaseConnectionString, "research.InsertHotIdea", parameters);

            return ReturnValue;
        }

        public static IEnumerable<HotIdea> GetHotIdeas(string Username)
        {
            List<HotIdea> ReturnValue = new List<HotIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Username", Username));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetHotIdeas", parameters);

            foreach (dynamic row in reader)
            {
                var idea = new HotIdea
                {
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    Username = row.Username,
                    Symbol = row.Symbol,
                    StartDate = row.StartDate,
                    EndDate = row.EndDate,
                    UserId = row.UserId,
                    SecurityId = row.SecurityId,
                    IdeaAbstractShortTermViewId = row.IdeaAbstractShortTermViewId,
                    HotIdeaAge = row.HotIdeaAge
                };
                ReturnValue.Add(idea);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<IdeaAbstractLongTermView> GetIdeaAbstractLongTermViews()
        {
            List<IdeaAbstractLongTermView> ReturnValue = new List<IdeaAbstractLongTermView>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                    (Startup.HoloceneDatabaseConnectionString,
                    "research.GetIdeaAbstractLongTermViews", null);

            foreach (dynamic row in reader)
            {
                var rec = new IdeaAbstractLongTermView
                {
                    IdeaAbstractLongTermViewId = row.IdeaAbstractLongTermViewId,
                    Code = row.Code,
                    Desc = row.Desc,
                    Weighting = row.Weighting
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<IdeaAbstractShortTermView> GetIdeaAbstractShortTermViews()
        {
            List<IdeaAbstractShortTermView> ReturnValue = new List<IdeaAbstractShortTermView>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                    (Startup.HoloceneDatabaseConnectionString,
                    "research.GetIdeaAbstractShortTermViews", null);

            foreach (dynamic row in reader)
            {
                var rec = new IdeaAbstractShortTermView
                {
                    IdeaAbstractShortTermViewId = row.IdeaAbstractShortTermViewId,
                    Code = row.Code,
                    Desc = row.Desc,
                    Weighting = row.Weighting
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<IdeaAbstract> GetIdeaAbstracts(string Symbol, string Username, string Timestamp)
        {
            List<IdeaAbstract> ReturnValue = new List<IdeaAbstract>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }
            if (!string.IsNullOrEmpty(Username))
            {
                parameters.Add(new SqlParameter("@Username", Username));
            }
            if (!string.IsNullOrEmpty(Timestamp))
            {
                parameters.Add(new SqlParameter("@Timestamp", Convert.ToDateTime(Timestamp)));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                (Startup.HoloceneDatabaseConnectionString,
                "research.GetIdeaAbstracts", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new IdeaAbstract
                {
                    IdeaAbstractId = row.IdeaAbstractId,
                    DisplayName = row.DisplayName,
                    FirstName = row.FirstName,
                    LastName = row.LastName,
                    Username = row.Username,
                    Symbol = row.Symbol,
                    LongTermViewCode = row.LongTermViewCode,
                    LongTermViewDesc = row.LongTermViewDesc,
                    LongTermViewWeighting = row.LongTermViewWeighting,
                    ShortTermViewCode = row.ShortTermViewCode,
                    ShortTermViewDesc = row.ShortTermViewDesc,
                    ShortTermViewWeighting = row.ShortTermViewWeighting,
                    LongTermThesis = row.LongTermThesis,
                    ShortTermThesis = row.ShortTermThesis,
                    IsHotIdea = Convert.ToBoolean(row.IsHotIdea),
                    IdeaAbstractAge = row.IdeaAbstractAge
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<IdeaAbstract> GetIdeaAbstractHistory(string Symbol, string Username)
        {
            List<IdeaAbstract> ReturnValue = new List<IdeaAbstract>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            if (!string.IsNullOrEmpty(Username))
            {
                parameters.Add(new SqlParameter("@Username", Username));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                (Startup.HoloceneDatabaseConnectionString,
                "research.GetIdeaAbstractHistory", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new IdeaAbstract
                {
                    IdeaAbstractId = row.IdeaAbstractId,
                    BusDate = Convert.ToDateTime(row.BusDate),
                    DisplayName = row.DisplayName,
                    FirstName = row.FirstName,
                    LastName = row.LastName,
                    Username = row.Username,
                    Symbol = row.Symbol,
                    LongTermViewCode = row.LongTermViewCode,
                    LongTermViewDesc = row.LongTermViewDesc,
                    LongTermViewWeighting = row.LongTermViewWeighting,
                    ShortTermViewCode = row.ShortTermViewCode,
                    ShortTermViewDesc = row.ShortTermViewDesc,
                    ShortTermViewWeighting = row.ShortTermViewWeighting,
                    LongTermThesis = row.LongTermThesis,
                    ShortTermThesis = row.ShortTermThesis,
                    IdeaAbstractAge = row.IdeaAbstractAge
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<AnalystIdea> GetLatestEVandIdeaAbstract(string Symbol)
        {
            List<AnalystIdea> ReturnValue = new List<AnalystIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet
                (Startup.HoloceneDatabaseConnectionString,
                "research.GetLatestEVandIdeaAbstract", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new AnalystIdea
                {
                    UserId = row.UserId,
                    SecurityId = row.SecurityId,
                    ExpectedValueId = (row.ExpectedValueId is System.DBNull ? null : row.ExpectedValueId),
                    Desk = row.Desk,
                    ER = (row.ER is System.DBNull ? null : row.ER),
                    EV = (row.EV is System.DBNull ? null : row.EV),
                    EVAge = (row.EVAge is System.DBNull ? null : row.EVAge),
                    IdeaAge = (row.IdeaAge is System.DBNull ? null : row.IdeaAge),
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    Direction = row.Direction,
                    DirectionDesc = row.DirectionDesc,
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    IdeaAbstractLongTermViewCode = row.IdeaAbstractLongTermViewCode,
                    IdeaAbstractLongTermViewDesc = row.IdeaAbstractLongTermViewDesc,
                    IdeaAbstractShortTermThesis = row.IdeaAbstractShortTermThesis,
                    IdeaAbstractLongTermThesis = row.IdeaAbstractLongTermThesis,
                    IsHotIdea = Convert.ToBoolean(row.IsHotIdea),
                    HotIdeaAge = (row.HotIdeaAge is System.DBNull ? null : row.HotIdeaAge),
                    PreviousIdeaAbstractShortTermViewCode = row.PreviousIdeaAbstractShortTermViewCode,
                    PreviousIdeaAbstractShortTermViewDesc = row.PreviousIdeaAbstractShortTermViewDesc,
                    IsOppositeSide = row.IsOppositeSide,
                    PortfolioTimestamp = (row.PortfolioTimestamp is System.DBNull ? null : row.PortfolioTimestamp),
                    SEMV = (row.SEMV is System.DBNull ? null : row.SEMV),
                    SEMVFund = (row.SEMVFund is System.DBNull ? null : row.SEMVFund),
                    DollarIdio = (row.DollarIdio is System.DBNull ? null : row.DollarIdio),
                    DollarIdioFund = (row.DollarIdioFund is System.DBNull ? null : row.DollarIdioFund),
                    ThesisId = row.ThesisId,
                    ThesisCreatedOn = row.ThesisCreatedOn is DBNull ? null : row.ThesisCreatedOn,
                    ThesisText = row.ThesisText
                };

                ReturnValue.Add(rec);
            }

            return ReturnValue.AsEnumerable();
        }               

        public static int? SubmitEarningsPreview(Preview preview, 
            List<PreviewMetric> metrics, ExpectedValue ev, ExpectedValue evFTE,
            string AdditionalNotes, 
            string Username, string Filename)
        {
            //bool ReturnValue = true;
            int? EarningsPreviewId;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.Symbol },
                    new SqlParameter() {ParameterName = "@Username",
                        SqlDbType = SqlDbType.NVarChar, Value = Username },
                    new SqlParameter() {ParameterName = "@Status",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.Status },
                    new SqlParameter() {ParameterName = "@Filename",
                        SqlDbType = SqlDbType.NVarChar, Value = Filename },
                    new SqlParameter() {ParameterName = "@EarningsDate",
                        SqlDbType = SqlDbType.Date, Value = preview.EarningsDate },
                    new SqlParameter() {ParameterName = "@EarningsTime",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.EarningsTime },
                    new SqlParameter() {ParameterName = "@CurrentSEMVDesk",
                        SqlDbType = SqlDbType.Decimal, Value = preview.CurrentSEMVDesk },
                    new SqlParameter() {ParameterName = "@CurrentSEMVFund",
                        SqlDbType = SqlDbType.Decimal, Value = preview.CurrentSEMVFund },
                    new SqlParameter() {ParameterName = "@RecommendedSEMVDesk",
                        SqlDbType = SqlDbType.Decimal, Value = preview.RecommendedSEMVDesk },
                    new SqlParameter() {ParameterName = "@RecommendationDesk",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.RecommendationDesk },
                    new SqlParameter() {ParameterName = "@EVLive",
                        SqlDbType = SqlDbType.Decimal, Value = ev.Price },
                    new SqlParameter() {ParameterName = "@ERLive",
                        SqlDbType = SqlDbType.Decimal, Value = ev.Upside },
                    new SqlParameter() {ParameterName = "@ThesisTypeId",
                        SqlDbType = SqlDbType.Int, Value = preview.ThesisTypeId },
                    new SqlParameter() { ParameterName = "@ThesisNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.ThesisNotes },
                    new SqlParameter() {ParameterName = "@ConvictionLevelId",
                        SqlDbType = SqlDbType.Int, Value = preview.ConvictionLevelId },
                    new SqlParameter() {ParameterName = "@MgmtInteractionId",
                        SqlDbType = SqlDbType.Int, Value = preview.MgmtInteractionId },
                    new SqlParameter() {ParameterName = "@NumbersExpectationsId",
                        SqlDbType = SqlDbType.Int, Value = preview.NumbersExpectationsId },
                    new SqlParameter() {ParameterName = "@BigPictureNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.BigPictureNotes },
                    new SqlParameter() {ParameterName = "@PrintOutlookNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.PrintOutlookNotes },
                    new SqlParameter() {ParameterName = "@ConsensusId",
                        SqlDbType = SqlDbType.Int, Value = preview.ConsensusId },
                    new SqlParameter() {ParameterName = "@ShortInterestId",
                        SqlDbType = SqlDbType.Int, Value = preview.ShortInterestId },
                    new SqlParameter() {ParameterName = "@KeyRisksNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.KeyRisksNotes },
                    new SqlParameter() {ParameterName = "@ExitPositionNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.ExitPositionNotes },
                    new SqlParameter() {ParameterName = "@PctVolMoveIfWrong",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.PctVolMoveIfWrong },
                    new SqlParameter() {ParameterName = "@OptionsImpliedVol",
                        SqlDbType = SqlDbType.Decimal, Value = preview.OptionsImpliedVol },
                    new SqlParameter() {ParameterName = "@MgmtCheckId",
                        SqlDbType = SqlDbType.Int, Value = preview.MgmtCheckId },
                    new SqlParameter() {ParameterName = "@MgmtCheckNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.MgmtCheckNotes },
                    new SqlParameter() {ParameterName = "@DataCheckId",
                        SqlDbType = SqlDbType.Int, Value = preview.DataCheckId },
                    new SqlParameter() {ParameterName = "@DataCheckNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.DataCheckNotes },
                    new SqlParameter() {ParameterName = "@ExpertCheckId",
                        SqlDbType = SqlDbType.Int, Value = preview.ExpertCheckId },
                    new SqlParameter() {ParameterName = "@ExpertCheckNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.ExpertCheckNotes },
                    new SqlParameter() {ParameterName = "@CompsReadThruCheckId",
                        SqlDbType = SqlDbType.Int, Value = preview.CompsReadThruCheckId },
                    new SqlParameter() {ParameterName = "@CompsReadThruCheckNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.CompsReadThruCheckNotes },
                    new SqlParameter() {ParameterName = "@ReviewTranscriptsId",
                        SqlDbType = SqlDbType.Int, Value = preview.ReviewTranscriptsId },
                    new SqlParameter() {ParameterName = "@ReviewTranscriptsNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.ReviewTranscriptsNotes },
                    new SqlParameter() {ParameterName = "@ReviewSellSideId",
                        SqlDbType = SqlDbType.Int, Value = preview.ReviewSellSideId },
                    new SqlParameter() {ParameterName = "@ReviewSellSideNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.ReviewSellSideNotes },
                    new SqlParameter() {ParameterName = "@TeamReviewId",
                        SqlDbType = SqlDbType.Int, Value = preview.TeamReviewId },
                    new SqlParameter() {ParameterName = "@TeamReviewNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.TeamReviewNotes },
                    new SqlParameter() {ParameterName = "@SetupPostEarningsCallId",
                        SqlDbType = SqlDbType.Int, Value = preview.SetupPostEarningsCallId },
                    new SqlParameter() {ParameterName = "@SetupPostEarningsCallNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.SetupPostEarningsCallNotes },
                    new SqlParameter() {ParameterName = "@AdditionalNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = AdditionalNotes },
                    new SqlParameter() {ParameterName = "@PostTradeNotes",
                        SqlDbType = SqlDbType.NVarChar, Value = preview.PostTradeNotes },
                    new SqlParameter() {ParameterName = "@EarningsPreviewId",
                        SqlDbType = SqlDbType.Int, Value = preview.EarningsPreviewId,
                        Direction = ParameterDirection.InputOutput }
                };

            if (ev.ExpectedValueId != 0)
            {
                parameters.Add(new SqlParameter()
                {
                    ParameterName = "@ExpectedValueId",
                    SqlDbType = SqlDbType.Int,
                    Value = ev.ExpectedValueId
                });
            }
            if (evFTE.ExpectedValueId != 0)
            {
                parameters.Add(new SqlParameter()
                {
                    ParameterName = "@ExpectedValueIdFTE",
                    SqlDbType = SqlDbType.Int,
                    Value = evFTE.ExpectedValueId
                });
            }

            EarningsPreviewId = ExecuteStoredProcedureWithReturnValue(
                Startup.HoloceneDatabaseConnectionString,
                "research.SubmitEarningsPreview", parameters);

            if (EarningsPreviewId.HasValue)
            {
                foreach (PreviewMetric metric in metrics)
                {
                    parameters = new List<SqlParameter>() {
                            new SqlParameter() {ParameterName = "@EarningsPreviewId",
                                SqlDbType = SqlDbType.BigInt, Value = EarningsPreviewId.Value },
                            new SqlParameter() {ParameterName = "@KeyMetric",
                                SqlDbType = SqlDbType.NVarChar, Value = metric.KeyMetric },
                            new SqlParameter() {ParameterName = "@KeyMetricOther",
                                SqlDbType = SqlDbType.NVarChar, Value = metric.KeyMetricOther },
                            new SqlParameter() {ParameterName = "@HoaEst",
                                SqlDbType = SqlDbType.Decimal, Value = metric.HoaEst },
                            new SqlParameter() {ParameterName = "@ConsensusEst",
                                SqlDbType = SqlDbType.Decimal, Value = metric.ConsensusEst } };

                    ExecuteStoredProcedure(
                        Startup.HoloceneDatabaseConnectionString,
                        "research.SubmitEarningsPreviewMetric",
                        parameters);
                }
            }
            else
            {
                throw new Exception("Error submitting earnings preview");
            }

            return EarningsPreviewId;
        }

        public static IEnumerable<Preview> GetEarningsPreviews(
            string Symbol, string Username, int EarningsPreviewId)
        {
            List<Preview> ReturnValue = new List<Preview>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (EarningsPreviewId != 0)
            {
                parameters.Add(new SqlParameter("@EarningsPreviewId", EarningsPreviewId));
            }
            else
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
                parameters.Add(new SqlParameter("@Username", Username));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString, "research.GetEarningsPreviews", parameters);

            foreach (dynamic row in reader)
            {
                var preview = new Preview
                {
                    EarningsPreviewId = row.EarningsPreviewId,
                    BusDate = row.BusDate,
                    Symbol = row.Symbol,
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    EarningsFilename = row.Filename,
                    EarningsDate = (row.EarningsDate is System.DBNull ? null : row.EarningsDate),
                    EarningsTime = (row.EarningsTime is System.DBNull ? "" : row.EarningsTime),
                    CurrentSEMVDesk = (row.CurrentSEMVDesk is System.DBNull ? null : row.CurrentSEMVDesk),
                    CurrentSEMVFund = (row.CurrentSEMVFund is System.DBNull ? null : row.CurrentSEMVFund),
                    RecommendedSEMVDesk = (row.RecommendedSEMVDesk is System.DBNull ? null : row.RecommendedSEMVDesk),
                    RecommendationDesk = row.RecommendationDesk,
                    ExpectedValueId = (row.ExpectedValueId is System.DBNull ? null : row.ExpectedValueId),
                    ERLive = (row.ERLive is System.DBNull ? null : row.ERLive),
                    EVLive = (row.EVLive is System.DBNull ? null : row.EVLive),
                    ExpectedValueIdFTE = (row.ExpectedValueIdFTE is System.DBNull ? null : row.ExpectedValueIdFTE),
                    ThesisTypeId = (row.ThesisTypeId is System.DBNull ? null : row.ThesisTypeId),
                    ThesisNotes = (row.ThesisNotes is System.DBNull ? "" : row.ThesisNotes),
                    ConvictionLevelId = (row.ConvictionLevelId is System.DBNull ? null : row.ConvictionLevelId),
                    MgmtInteractionId = (row.MgmtInteractionId is System.DBNull ? null : row.MgmtInteractionId),
                    NumbersExpectationsId = (row.NumbersExpectationsId is System.DBNull ? null : row.NumbersExpectationsId),
                    BigPictureNotes = (row.BigPictureNotes is System.DBNull ? "" : row.BigPictureNotes),
                    PrintOutlookNotes = (row.PrintOutlookNotes is System.DBNull ? "" : row.PrintOutlookNotes),
                    ConsensusId = (row.ConsensusId is System.DBNull ? null : row.ConsensusId),
                    ShortInterestId = (row.ShortInterestId is System.DBNull ? null : row.ShortInterestId),
                    KeyRisksNotes = (row.KeyRisksNotes is System.DBNull ? "" : row.KeyRisksNotes),
                    ExitPositionNotes = (row.ExitPositionNotes is System.DBNull ? "" : row.ExitPositionNotes),
                    PctVolMoveIfWrong = (row.PctVolMoveIfWrong is System.DBNull ? "" : row.PctVolMoveIfWrong),
                    PctVolMoveIfWrongId = (row.PctVolMoveIfWrongId is System.DBNull ? null : row.PctVolMoveIfWrongId),
                    OptionsImpliedVol = (row.OptionsImpliedVol is System.DBNull ? null : row.OptionsImpliedVol),
                    PctSURP = (row.PctSURP is System.DBNull ? null : row.PctSURP),
                    MgmtCheckId = (row.MgmtCheckId is System.DBNull ? null : row.MgmtCheckId),
                    MgmtCheckNotes = (row.MgmtCheckNotes is System.DBNull ? "" : row.MgmtCheckNotes),
                    DataCheckId = (row.DataCheckId is System.DBNull ? null : row.DataCheckId),
                    DataCheckNotes = (row.DataCheckNotes is System.DBNull ? "" : row.DataCheckNotes),
                    ExpertCheckId = (row.ExpertCheckId is System.DBNull ? null : row.ExpertCheckId),
                    ExpertCheckNotes = (row.ExpertCheckNotes is System.DBNull ? "" : row.ExpertCheckNotes),
                    CompsReadThruCheckId = (row.CompsReadThruCheckId is System.DBNull ? null : row.CompsReadThruCheckId),
                    CompsReadThruCheckNotes = (row.CompsReadThruCheckNotes is System.DBNull ? "" : row.CompsReadThruCheckNotes),
                    ReviewTranscriptsId = (row.ReviewTranscriptsId is System.DBNull ? null : row.ReviewTranscriptsId),
                    ReviewTranscriptsNotes = (row.ReviewTranscriptsNotes is System.DBNull ? "" : row.ReviewTranscriptsNotes),
                    ReviewSellSideId = (row.ReviewSellSideId is System.DBNull ? null : row.ReviewSellSideId),
                    ReviewSellSideNotes = (row.ReviewSellSideNotes is System.DBNull ? "" : row.ReviewSellSideNotes),
                    TeamReviewId = (row.TeamReviewId is System.DBNull ? null : row.TeamReviewId),
                    TeamReviewNotes = (row.TeamReviewNotes is System.DBNull ? "" : row.TeamReviewNotes),
                    SetupPostEarningsCallId = (row.SetupPostEarningsCallId is System.DBNull ? null : row.SetupPostEarningsCallId),
                    SetupPostEarningsCallNotes = (row.SetupPostEarningsCallNotes is System.DBNull ? "" : row.SetupPostEarningsCallNotes),
                    AdditionalNotes = (row.AdditionalNotes is System.DBNull ? "" : row.AdditionalNotes),
                    PostTradeNotes = (row.PostTradeNotes is System.DBNull ? "" : row.PostTradeNotes),
                    Status = row.Status,
                    Timestamp = row.ModifiedOn
                };

                ReturnValue.Add(preview);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<PreviewMetric> GetEarningsPreviewMetrics(
            int EarningsPreviewId)
        {
            List<PreviewMetric> ReturnValue = new List<PreviewMetric>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (EarningsPreviewId != 0)
            {
                parameters.Add(new SqlParameter("@EarningsPreviewId", EarningsPreviewId));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString, "research.GetEarningsPreviewMetrics", parameters);

            foreach (dynamic row in reader)
            {
                var previewMetric = new PreviewMetric
                {
                    EarningsPreviewId = row.EarningsPreviewId,
                    KeyMetric = row.RefName,
                    KeyMetricOther = (row.KeyMetricOther is System.DBNull ? string.Empty :
                        row.KeyMetricOther),
                    HoaEst = (row.HoaEst is System.DBNull ? null : row.HoaEst),
                    ConsensusEst = (row.ConsensusEst is System.DBNull ? null : row.ConsensusEst)
                };

                ReturnValue.Add(previewMetric);
            }

            return ReturnValue.AsEnumerable();
        }

        public static bool SnoozePreview(string Symbol, string EarningsDate, string Username, string Notes)
        {
            List<ExpectedValueScenario> ReturnValue = new List<ExpectedValueScenario>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            parameters.Add(new SqlParameter("@EarningsDate", EarningsDate));
            parameters.Add(new SqlParameter("@Username", Username));
            parameters.Add(new SqlParameter("@Notes", Notes));

            ExecuteStoredProcedure(
                Startup.HoloceneDatabaseConnectionString,
                "research.SnoozePreview", parameters);

            return true;
        }

        public static List<StreetEvent> GetStreetEvents(string EventTypeCode)
        {
            List<StreetEvent> ReturnValue = new List<StreetEvent>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@EventTypeCode", SqlDbType = SqlDbType.NVarChar,
                        Value = EventTypeCode }
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "core.GetStreetEvents", parameters);

            foreach (dynamic row in reader)
            {
                var streetEvent = new StreetEvent
                {
                    StreetEventId = Convert.ToInt64(row.StreetEventId),
                    SnapshotDate = Convert.ToDateTime(row.SnapshotDate),
                    Symbol = row.Symbol,
                    EventTypeCode = row.EventTypeCode,
                    EventTypeDesc = row.EventTypeDesc,
                    EventDate = Convert.ToDateTime(row.EventDate),
                    EventTime = row.EventTime
                };
                ReturnValue.Add(streetEvent);
            }

            return ReturnValue;
        }

        public static BarraSecurityRisk GetBarraMetrics(string BarraId)
        {
            BarraSecurityRisk metric = new BarraSecurityRisk();

            try
            {
                string RawSql = "select barraid,date,pred_beta,specific_risk/100 specific_risk" +
                    " from public.usmeds_asset_data_last" +
                    " where barraid in ('" + BarraId + "')";

                var reader = PostgresExecuteStoredProcedureWithResultSet
                    (Startup.QRDatabaseConnectionString, RawSql);

                foreach (dynamic row in reader)
                {
                    metric.BarraId = row.barraid;
                    metric.PredBeta = row.pred_beta is System.DBNull ? 0 : Convert.ToDecimal(row.pred_beta);
                    metric.SpecRisk = row.specific_risk is System.DBNull ? 0 : Convert.ToDecimal(row.specific_risk);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }

            return metric;
        }

        public static List<AltDataDashboardDetail> GetAltDataDashboardDetails()
        {
            List<AltDataDashboardDetail> ReturnValue = new List<AltDataDashboardDetail>();

            string RawSql = "select * from dashboard.url_info where in_production = 1 order by datasource,name";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataDashboardDetail
                {
                    Name = row.name,
                    URL = (row.url is System.DBNull ? string.Empty : row.url),
                    Description = row.description,
                    DataSource = row.datasource,
                    UnderlyingTable = row.underlying_table,
                    IsActive = Convert.ToInt32(row.in_production),
                    Frequency = row.frequency,
                    View = (row.view_table is System.DBNull ? string.Empty : row.view_table),
                    DefaultMetric = (row.default_metric is System.DBNull ? string.Empty : row.default_metric),
                    DefaultFilter1 = (row.default_filter1 is System.DBNull ? string.Empty : row.default_filter1),
                    DefaultFilter2 = (row.default_filter2 is System.DBNull ? string.Empty : row.default_filter2),
                    DefaultFilter3 = (row.default_filter3 is System.DBNull ? string.Empty : row.default_filter3),
                    DefaultFilter4 = (row.default_filter4 is System.DBNull ? string.Empty : row.default_filter4)
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static List<IdeaEngineStatus> GetIdeaEngineStatuses()
        {
            List<IdeaEngineStatus> ReturnValue = new List<IdeaEngineStatus>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "research.GetIdeaEngineStatuses", null);

            foreach (dynamic row in reader)
            {
                var status = new IdeaEngineStatus
                {
                    IdeaEngineStatusId = Convert.ToInt32(row.IdeaEngineStatusId),
                    Code = row.Code,
                    Desc = row.Desc,
                    DisplayOrder = row.DisplayOrder,
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
                ReturnValue.Add(status);
            }

            return ReturnValue;
        }

        public static List<IdeaEngineSize> GetIdeaEngineSize()
        {
            List<IdeaEngineSize> ReturnValue = new List<IdeaEngineSize>();

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "research.GetIdeaEngineSize", null);

            foreach (dynamic row in reader)
            {
                var size = new IdeaEngineSize
                {
                    IdeaEngineSizeId = Convert.ToInt32(row.IdeaEngineSizeId),
                    Code = row.Code,
                    Desc = row.Desc,
                    DisplayOrder = row.DisplayOrder,
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
                ReturnValue.Add(size);
            }

            return ReturnValue;
        }

        public static List<IdeaEngine> GetIdeaEngine(string Symbol, string Topic, string Username,
            string RequestType, Int64 EngineId)
        {
            List<IdeaEngine> ReturnValue = new List<IdeaEngine>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }
            if (!string.IsNullOrEmpty(Topic))
            {
                parameters.Add(new SqlParameter("@Topic", Topic));
            }
            if (!string.IsNullOrEmpty(Username))
            {
                parameters.Add(new SqlParameter("@Username", Username));
            }
            if (!string.IsNullOrEmpty(RequestType))
            {
                parameters.Add(new SqlParameter("@RequestType", Symbol));
            }
            if (EngineId != 0)
            {
                parameters.Add(new SqlParameter("@IdeaEngineId", EngineId));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetIdeaEngine2", parameters);

            foreach (dynamic row in reader)
            {
                var engine = new IdeaEngine
                {
                    IdeaEngineId = Convert.ToInt64(row.IdeaEngineId),
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    SymbolOrTopic = row.SymbolOrTopic,
                    Topic = row.Topic,
                    Symbol = row.Symbol,
                    BusDate =
                        (row.BusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.BusDate)),
                    Thesis = row.Thesis,
                    ActionItem1 = row.ActionItem1,
                    ActionItem2 = row.ActionItem2,
                    ActionItem3 = row.ActionItem3,
                    ActionItem4 = row.ActionItem4,
                    ActionItem5 = row.ActionItem5,
                    ActionItem6 = row.ActionItem6,
                    AssignBusDate =
                        (row.AssignBusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.AssignBusDate)),
                    AssignedByUsername = row.AssignedByUsername,
                    AssignedByDisplayName = row.AssignedByDisplayName,
                    SizeCode = row.SizeCode,
                    SizeDesc = row.SizeDesc,
                    StatusCode = row.StatusCode,
                    StatusDesc = row.StatusDesc,
                    Conclusion1 = row.Conclusion1,
                    Conclusion2 = row.Conclusion2,
                    Conclusion3 = row.Conclusion3,
                    Sector = row.Sector,
                    Desk = row.Desk,
                    DeskShort = row.DeskShort,
                    InvestmentType = row.InvestmentType,
                    IdeaRecommendation = row.IdeaRecommendation,
                    InvestmentTypeDesc = row.InvestmentTypeDesc,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc
                };
                ReturnValue.Add(engine);
            }

            return ReturnValue;
        }

        public static List<IdeaEngine> GetIdeaEngineById(Int64 EngineId)
        {
            List<IdeaEngine> ReturnValue = new List<IdeaEngine>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (EngineId != 0)
            {
                parameters.Add(new SqlParameter("@IdeaEngineId", EngineId));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetIdeaEngineById", parameters);

            foreach (dynamic row in reader)
            {
                var engine = new IdeaEngine
                {
                    IdeaEngineId = Convert.ToInt64(row.IdeaEngineId),
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    SymbolOrTopic = row.SymbolOrTopic,
                    Topic = row.Topic,
                    Symbol = row.Symbol,
                    BusDate =
                        (row.BusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.BusDate)),
                    Thesis = row.Thesis,
                    ActionItem1 = row.ActionItem1,
                    ActionItem2 = row.ActionItem2,
                    ActionItem3 = row.ActionItem3,
                    ActionItem4 = row.ActionItem4,
                    ActionItem5 = row.ActionItem5,
                    ActionItem6 = row.ActionItem6,
                    AssignBusDate =
                        (row.AssignBusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.AssignBusDate)),
                    AssignedByUsername = row.AssignedByUsername,
                    AssignedByDisplayName = row.AssignedByDisplayName,
                    SizeCode = row.SizeCode,
                    SizeDesc = row.SizeDesc,
                    StatusCode = row.StatusCode,
                    StatusDesc = row.StatusDesc,
                    Conclusion1 = row.Conclusion1,
                    Conclusion2 = row.Conclusion2,
                    Conclusion3 = row.Conclusion3,
                    Sector = row.Sector,
                    Desk = row.Desk,
                    DeskShort = row.DeskShort,
                    InvestmentType = row.InvestmentType,
                    IdeaRecommendation = row.IdeaRecommendation,
                    InvestmentTypeDesc = row.InvestmentTypeDesc,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc
                };
                ReturnValue.Add(engine);
            }

            return ReturnValue;
        }

        public static List<IdeaEngine> GetIdeaEngineList(string Symbol, string Topic, Int64 EngineId)
        {
            List<IdeaEngine> ReturnValue = new List<IdeaEngine>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }
            if (!string.IsNullOrEmpty(Topic))
            {
                parameters.Add(new SqlParameter("@Topic", Topic));
            }
            if (EngineId != 0)
            {
                parameters.Add(new SqlParameter("@IdeaEngineId", EngineId));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetIdeaEngineList", parameters);

            foreach (dynamic row in reader)
            {
                var engine = new IdeaEngine
                {
                    IdeaEngineId = Convert.ToInt64(row.IdeaEngineId),
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    SymbolOrTopic = row.SymbolOrTopic,
                    Topic = row.Topic,
                    Symbol = row.Symbol,
                    BusDate =
                        (row.BusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.BusDate)),
                    Thesis = row.Thesis,
                    AssignBusDate =
                        (row.AssignBusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.AssignBusDate)),
                    AssignedByUsername = row.AssignedByUsername,
                    AssignedByDisplayName = row.AssignedByDisplayName,
                    SizeCode = row.SizeCode,
                    SizeDesc = row.SizeDesc,
                    StatusCode = row.StatusCode,
                    StatusDesc = row.StatusDesc,
                    StatusShortDesc = row.StatusShortDesc,
                    Sector = row.Sector,
                    Desk = row.Desk,
                    DeskShort = row.DeskShort
                };
                ReturnValue.Add(engine);
            }

            return ReturnValue;
        }

        public static IdeaEngine UpdateIdeaEngine(IdeaEngine engine, string AssignedByUsername)
        {
            IdeaEngine ReturnValue = new IdeaEngine();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (engine.IdeaEngineId != 0)
            {
                parameters.Add(new SqlParameter("@IdeaEngineId", engine.IdeaEngineId));
            }
            if (!string.IsNullOrEmpty(engine.Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", engine.Symbol));
            }
            if (!string.IsNullOrEmpty(engine.Topic))
            {
                parameters.Add(new SqlParameter("@Topic", engine.Topic));
            }
            if (!string.IsNullOrEmpty(engine.Username))
            {
                parameters.Add(new SqlParameter("@Username", engine.Username));
            }
            if (!string.IsNullOrEmpty(AssignedByUsername))
            {
                parameters.Add(new SqlParameter("@AssignedByUsername", AssignedByUsername));
            }
            if (!string.IsNullOrEmpty(engine.Thesis))
            {
                parameters.Add(new SqlParameter("@Thesis", engine.Thesis));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem1))
            {
                parameters.Add(new SqlParameter("@ActionItem1", engine.ActionItem1));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem2))
            {
                parameters.Add(new SqlParameter("@ActionItem2", engine.ActionItem2));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem3))
            {
                parameters.Add(new SqlParameter("@ActionItem3", engine.ActionItem3));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem4))
            {
                parameters.Add(new SqlParameter("@ActionItem4", engine.ActionItem4));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem5))
            {
                parameters.Add(new SqlParameter("@ActionItem5", engine.ActionItem5));
            }
            if (!string.IsNullOrEmpty(engine.ActionItem6))
            {
                parameters.Add(new SqlParameter("@ActionItem6", engine.ActionItem6));
            }
            if (!string.IsNullOrEmpty(engine.SizeCode))
            {
                parameters.Add(new SqlParameter("@SizeCode", engine.SizeCode));
            }
            if (!string.IsNullOrEmpty(engine.StatusCode))
            {
                parameters.Add(new SqlParameter("@StatusCode", engine.StatusCode));
            }
            if (!string.IsNullOrEmpty(engine.Conclusion1))
            {
                parameters.Add(new SqlParameter("@Conclusion1", engine.Conclusion1));
            }
            if (!string.IsNullOrEmpty(engine.Conclusion2))
            {
                parameters.Add(new SqlParameter("@Conclusion2", engine.Conclusion2));
            }
            if (!string.IsNullOrEmpty(engine.Conclusion3))
            {
                parameters.Add(new SqlParameter("@Conclusion3", engine.Conclusion3));
            }
            if (!string.IsNullOrEmpty(engine.InvestmentType))
            {
                parameters.Add(new SqlParameter("@InvestmentType", engine.InvestmentType));
            }
            if (!string.IsNullOrEmpty(engine.IdeaRecommendation))
            {
                parameters.Add(new SqlParameter("@IdeaRecommendation",
                    engine.IdeaRecommendation));
            }

            Int64? ideaEngineId = ExecuteStoredProcedureWithReturnValue(
                Startup.HoloceneDatabaseConnectionString,
                "research.UpdateIdeaEngine2", parameters);

            if (ideaEngineId.HasValue && ideaEngineId.Value != 0)
            {
                List<IdeaEngine> lstEngine = DatabaseHelper
                    .GetIdeaEngineById(ideaEngineId.Value).ToList();
                if (lstEngine.Count != 0)
                    ReturnValue = lstEngine.First();
            }

            return ReturnValue;
        }

        public static List<BarraRelatedSecurity> GetBarraRelatedSecurities(string Symbol)
        {
            List<BarraRelatedSecurity> ReturnValue = new List<BarraRelatedSecurity>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "risk.GetBarraRelatedSecurities", parameters);

            foreach (dynamic row in reader)
            {
                var sec = new BarraRelatedSecurity
                {
                    Symbol = row.Symbol,
                    BarraId = row.BarraId,
                    RelatedSymbol = row.RelatedSymbol,
                    Loading = Convert.ToDecimal(row.Loading)
                };
                ReturnValue.Add(sec);
            }

            return ReturnValue;
        }

        public static List<FactorLoading> GetBarraFactorLoadingsHistory(string Symbol,
            string Factor)
        {
            List<FactorLoading> ReturnValue = new List<FactorLoading>();

            List<SqlParameter> parameters = new List<SqlParameter>();

            if (!string.IsNullOrEmpty(Symbol))
            {
                parameters.Add(new SqlParameter("@Symbol", Symbol));
            }
            if (!string.IsNullOrEmpty(Factor))
            {
                parameters.Add(new SqlParameter("@Factor", Factor));
            }

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "risk.GetBarraFactorsHistory", parameters);

            foreach (dynamic row in reader)
            {
                var sec = new FactorLoading
                {
                    Symbol = row.Symbol,
                    BarraId = row.BarraId,
                    BusDate = Convert.ToDateTime(row.BusDate),
                    FactorGroup = row.FactorGroup,
                    FactorName = row.Factor,
                    FactorDesc = row.FactorDesc,
                    Loading = Convert.ToDecimal(row.Loading)
                };
                ReturnValue.Add(sec);
            }

            return ReturnValue;
        }

        public static List<PreviewReference> GetPreviewReference()
        {
            List<PreviewReference> ReturnValue = new List<PreviewReference>();

            IEnumerable<dynamic> reader = ExecuteInlineQueryWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "select * from research.EarningsPreviewReference (nolock)");

            foreach (dynamic row in reader)
            {
                var reference = new PreviewReference
                {
                    EarningsPreviewReferenceId = row.EarningsPreviewReferenceId,
                    RefGroup = row.RefGroup,
                    RefName = row.RefName,
                    RefValue = row.RefValue,
                    IsActive = row.IsActive is System.DBNull ? null : Convert.ToBoolean(row.IsActive),
                    DisplayOrder = row.DisplayOrder is System.DBNull ? null : Convert.ToInt32(row.DisplayOrder)
                };
                ReturnValue.Add(reference);
            }

            return ReturnValue;
        }        

        public static IEnumerable<AltDataBreakdown> GetAltDataBreakdownView(string Ticker)
        {
            List<AltDataBreakdown> ReturnValue = new List<AltDataBreakdown>();

            string RawSql = "select * from core_catalog.breakdown_view where upper(asset_name) = '" +
                    Ticker.ToUpper() + "';";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataBreakdown
                {
                    BreakdownId = row.breakdown_id,
                    AssetId = row.asset_id,
                    DataSourceId = row.datasource_id,
                    MetricId = row.metric_id,
                    DataSourceProvider = row.datasource_provider,
                    DataSourceName = row.datasource_name,
                    DataSourceDesc = row.datasource_desc,
                    AssetName = row.asset_name,
                    MetricName = row.metric_name,
                    MetricDesc = row.metric_description,
                    MetricFrequency = row.metric_frequency,
                    MetricLag = row.metric_lag
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<AltDataFilter> GetAltDataFilterView(int BreakdownId)
        {
            List<AltDataFilter> ReturnValue = new List<AltDataFilter>();

            string RawSql = "select * from core_mapping.filter_view where breakdown_id = " +
                    BreakdownId + ";";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataFilter
                {
                    FilterId = row.filter_id,
                    FilterDetailId = row.filter_detail_id,
                    CatalogDetailId = row.catalog_detail_id,
                    CatalogId = row.catalog_id,
                    BreakdownId = row.breakdown_id,
                    FilterDesc = row.filter_description,
                    CatalogName = row.catalog_name,
                    CatalogValue = row.catalog_value
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<AltDataPeriodTypeStat> GetAltDataPeriodTypeStatView(int BreakdownId)
        {
            List<AltDataPeriodTypeStat> ReturnValue = new List<AltDataPeriodTypeStat>();

            string RawSql = "select * from core_catalog.period_stat_view where breakdown_id = " +
                    BreakdownId + ";";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataPeriodTypeStat
                {
                    BreakdownId = row.breakdown_id,
                    PeriodTypeId = row.period_type_id,
                    StatId = row.stat_id,
                    PeriodTypeName = row.period_type_name,
                    StatName = row.stat_name,
                    StatDesc = row.stat_desc
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<AltDataRecord> GetAltDataRecordView(
            int DataSourceId, int AssetId, int MetricId, int FilterId, int StatId,
            int PeriodTypeId)
        {
            List<AltDataRecord> ReturnValue = new List<AltDataRecord>();

            string RawSql = "select * from core_mapping.record_view where" +
                    " datasource_id = " + DataSourceId +
                    " and asset_id = " + AssetId +
                    " and metric_id = " + MetricId +
                    (FilterId == 0 ? "" : " and filter_id = " + FilterId) +
                    " and stat_id = " + StatId +
                    " and period_type_id = " + PeriodTypeId +
                    " order by period_end;";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataRecord
                {
                    DataSourceId = row.datasource_id,
                    AssetId = row.asset_id,
                    MetricId = row.metric_id,
                    FilterId = row.filter_id,
                    PeriodId = row.period_id,
                    StatId = row.stat_id,
                    PeriodTypeName = row.period_type_name,
                    PeriodStart = Convert.ToDateTime(row.period_start),
                    PeriodEnd = Convert.ToDateTime(row.period_end),
                    StatName = row.stat_name,
                    StatDesc = row.stat_description,
                    Value = row.value,
                    IsActual = Convert.ToBoolean(row.is_actual),
                    UploadTimestamp = Convert.ToDateTime(row.upload_datetime),
                    ModifiedBy = row.modified_by
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<AltDataRecord> GetAltDataRecordView2(
            int DataSourceId, int AssetId, int MetricId, int FilterId, int StatId,
            int PeriodTypeId)
        {
            List<AltDataRecord> ReturnValue = new List<AltDataRecord>();

            string RawSql = "select * from core_mapping.record_view where" +
                    " datasource_id = " + DataSourceId +
                    " and asset_id = " + AssetId +
                    " and metric_id = " + MetricId +
                    (FilterId == 0 ? "" : " and filter_id = " + FilterId) +
                    " and stat_id = " + StatId +
                    " and period_type_id = " + PeriodTypeId +
                    " order by period_end;";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataRecord
                {
                    DataSourceId = row.datasource_id,
                    AssetId = row.asset_id,
                    MetricId = row.metric_id,
                    FilterId = row.filter_id,
                    PeriodId = row.period_id,
                    StatId = row.stat_id,
                    PeriodTypeName = row.period_type_name,
                    PeriodStart = Convert.ToDateTime(row.period_start),
                    PeriodEnd = Convert.ToDateTime(row.period_end),
                    StatName = row.stat_name,
                    StatDesc = row.stat_description,
                    Value = row.value,
                    IsActual = Convert.ToBoolean(row.is_actual),
                    UploadTimestamp = Convert.ToDateTime(row.upload_datetime),
                    ModifiedBy = row.modified_by
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<AltDataStat> GetAltDataStatView()
        {
            List<AltDataStat> ReturnValue = new List<AltDataStat>();

            string RawSql = "select * from core_mapping.stat;";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var detail = new AltDataStat
                {
                    StatId = row.stat_id,
                    StatName = row.name,
                    StatDesc = row.description
                };

                ReturnValue.Add(detail);
            }

            return ReturnValue;
        }

        public static IEnumerable<ExpectedValueScenario> GetExpectedValueNestedScenarios(int ExpectedValueId)
        {
            List<ExpectedValueScenario> ReturnValue = new List<ExpectedValueScenario>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@ExpectedValueId", ExpectedValueId));

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetExpectedValueNestedScenarios", parameters);

            foreach (dynamic row in reader)
            {
                var expectedValueScenario = new ExpectedValueScenario
                {
                    ExpectedValueId = row.ExpectedValueNestedId,
                    ExpectedValueScenarioId = row.ExpectedValueScenarioNestedId,
                    ScenarioNested = row.EVScenarioTypeCode,
                    Scenario = row.ScenarioTypeCode,
                    Metric = row.Metric,
                    Multiple = row.Multiple,
                    Probability = row.Probability,
                    Price = row.Price,
                    Upside = row.Upside,
                    Username = row.Username,
                    Notes = row.Notes,
                    BusDate = row.BusDate,
                    Timestamp = row.ModifiedOn,
                    Symbol = row.Symbol,
                    FullBloombergCode = row.FullBloombergCode,
                    ShortName = row.ShortName,
                    CINS = row.Cins,
                    CUSIP = row.Cusip,
                    MetricTypeCode = row.MetricTypeCode,
                    PeriodTypeCode = row.PeriodTypeCode,
                    IdeaRecommendationCode = row.IdeaRecommendationCode,
                    IdeaRecommendationDesc = row.IdeaRecommendationDesc,
                    OverrideCalc = Convert.ToBoolean(row.OverrideCalc),
                    CanUpdate = row.CanUpdate,
                    SecurityDesc = row.SecurityDesc
                };
                ReturnValue.Add(expectedValueScenario);
            }

            return ReturnValue.AsEnumerable();
        }

        public static Int64 UpdateNestedExpectedValueScenarios(
            ExpectedValue ExpectedValue, 
            List<ExpectedValueScenario> ExpectedValueScenarios, string Username,
            Int64 ParentExpectedValueId,
            string ParentScenarioType)
        {
            Int64 ReturnValue = 0;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@ParentExpectedValueId", SqlDbType = SqlDbType.Int, Value = ParentExpectedValueId },
                    new SqlParameter() {ParameterName = "@ParentScenarioType", SqlDbType = SqlDbType.NVarChar, Value = ParentScenarioType },
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.Symbol },
                    new SqlParameter() {ParameterName = "@SecurityDesc", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.SecurityDesc },
                    new SqlParameter() {ParameterName = "@FullBloombergCode", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.FullBloombergCode },
                    new SqlParameter() {ParameterName = "@ShortName", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.ShortName },
                    new SqlParameter() {ParameterName = "@CUSIP", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.CUSIP },
                    new SqlParameter() {ParameterName = "@CINS", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.CINS },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.Username },
                    new SqlParameter() {ParameterName = "@MetricType", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.MetricTypeCode },
                    new SqlParameter() {ParameterName = "@PeriodType", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.PeriodTypeCode },
                    new SqlParameter() {ParameterName = "@CQF", SqlDbType = SqlDbType.NVarChar, Value = "0" },
                    new SqlParameter() {ParameterName = "@IdeaRecommendationCode", SqlDbType = SqlDbType.NVarChar, Value = ExpectedValue.IdeaRecommendationCode },
                    new SqlParameter() {ParameterName = "@OverrideCalc", SqlDbType = SqlDbType.Int, Value = (ExpectedValue.OverrideCalc ? 1 : 0) },
                    new SqlParameter() {ParameterName = "@Price", SqlDbType = SqlDbType.Decimal, Value = ExpectedValue.Price },
                    new SqlParameter() {ParameterName = "@Upside", SqlDbType = SqlDbType.Decimal, Value = ExpectedValue.Upside },
                    new SqlParameter() {ParameterName = "@ExpectedValueId", SqlDbType = SqlDbType.Int, Value = ExpectedValue.ExpectedValueId, Direction = ParameterDirection.InputOutput }
                };

            ReturnValue = (int)ExecuteStoredProcedureWithReturnValue(
                Startup.HoloceneDatabaseConnectionString, "research.UpsertNestedExpectedValue", parameters);

            foreach (ExpectedValueScenario expScenario in ExpectedValueScenarios)
            {
                List<SqlParameter> parameters1 = new List<SqlParameter>() {
                        new SqlParameter() {ParameterName = "@ExpectedValueId", SqlDbType = SqlDbType.Int,
                            Value = ReturnValue },
                        new SqlParameter() {ParameterName = "@Scenario", SqlDbType = SqlDbType.NVarChar, Value = expScenario.Scenario },
                        new SqlParameter() {ParameterName = "@Metric", SqlDbType = SqlDbType.Decimal, Value = expScenario.Metric },
                        new SqlParameter() {ParameterName = "@Multiple", SqlDbType = SqlDbType.Decimal, Value = expScenario.Multiple },
                        new SqlParameter() {ParameterName = "@Probability", SqlDbType = SqlDbType.Decimal, Value = expScenario.Probability },
                        new SqlParameter() {ParameterName = "@Price", SqlDbType = SqlDbType.Decimal, Value = expScenario.Price },
                        new SqlParameter() {ParameterName = "@OverrideCalc", SqlDbType = SqlDbType.Int, Value = expScenario.OverrideCalc },
                        new SqlParameter() {ParameterName = "@Upside", SqlDbType = SqlDbType.Decimal, Value = expScenario.Upside },
                        new SqlParameter() {ParameterName = "@Notes", SqlDbType = SqlDbType.NVarChar, Value = expScenario.Notes },
                        new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username },
                        new SqlParameter() {ParameterName = "@ExpectedValueScenarioId", SqlDbType = SqlDbType.Int, Value = expScenario.ExpectedValueScenarioId, Direction = ParameterDirection.InputOutput }
                    };

                int expectedValueScenarioId = (int)ExecuteStoredProcedureWithReturnValue(
                    Startup.HoloceneDatabaseConnectionString,
                    "research.UpsertNestedExpectedValueScenario", parameters1);
            }

            return ReturnValue;
        }

        public static List<DashViewAsset> GetDashViews(string Topic)
        {
            List<DashViewAsset> ReturnValue = new List<DashViewAsset>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Topic", SqlDbType = SqlDbType.NVarChar, Value = Topic }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashViews", parameters);

            foreach (dynamic row in reader)
            {
                var view = new DashViewAsset
                {
                    DashViewAssetId = row.DashViewAssetId,
                    AssetId = row.AssetId,
                    AssetName = row.AssetName,
                    AssetType = row.AssetType,
                    DashViewId = row.DashViewId,
                    DashViewName = row.DashViewName,
                    DashViewDesc = row.DashViewDesc,
                    DashViewGroup = row.DashViewGroup,
                    IsActive = Convert.ToBoolean(row.IsActive),
                    DashboardUrl = (row.DashboardUrl is System.DBNull ? string.Empty : row.DashboardUrl),
                    DisplayOrder = (row.DisplayOrder is System.DBNull ? 0 : row.DisplayOrder)
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<DashViewAsset> GetDashViewsByID(Int64 ID)
        {
            List<DashViewAsset> ReturnValue = new List<DashViewAsset>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashAssetID", SqlDbType = SqlDbType.BigInt, Value = ID }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashViewsByID", parameters);

            foreach (dynamic row in reader)
            {
                var view = new DashViewAsset
                {
                    DashViewAssetId = row.DashViewAssetId,
                    AssetId = row.AssetId,
                    AssetName = row.AssetName,
                    AssetType = row.AssetType,
                    DashViewId = row.DashViewId,
                    DashViewName = row.DashViewName,
                    DashViewDesc = row.DashViewDesc,
                    IsActive = Convert.ToBoolean(row.IsActive),
                    DashboardUrl = (row.DashboardUrl is System.DBNull ? string.Empty : row.DashboardUrl),
                    DisplayOrder = (row.DisplayOrder is System.DBNull ? 0 : row.DisplayOrder)
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<DashViewCache> GetDashViewCache(long DashViewId)
        {
            List<DashViewCache> ReturnValue = new List<DashViewCache>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashViewId", SqlDbType = SqlDbType.BigInt, Value = DashViewId }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashViewChartsCache", parameters);

            foreach (dynamic row in reader)
            {
                var view = new DashViewCache
                {
                    DashViewId = row.DashViewId,
                    DashChartId = row.DashChartId,
                    DashQueryId = row.DashQueryId,
                    DashChartQueryId = row.DashChartQueryId,
                    Query = row.Query,
                    QueryName = row.QueryName,
                    QueryDesc = row.QueryDesc,
                    Aesthetic = row.Aesthetic,
                    CacheData = row.CacheData,
                    CacheModifiedDate = Convert.ToDateTime(row.CacheModifiedDate),
                    DisplayOrder = row.DisplayOrder
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<DashCatalog> SearchDataCatalog(string Query)
        {
            List<DashCatalog> ReturnValue = new List<DashCatalog>();

            string RawSql = "select * from data.span (nolock) where ";
            RawSql = RawSql + " (upper(ltrim(rtrim(description))) = '" + Query.ToUpper().Trim() + "')";
            string WhereClause = string.Empty;
            List<string> dimensions = new List<string>();

            string[] scSplit = Query.Split(";");
            foreach (string sc in scSplit)
            {
                string[] spaceSplit = sc.Split(" ");
                if (spaceSplit.Length == 0)
                {
                    string[] equalSplit = sc.Split("=");
                    if (equalSplit.Length == 0)
                    {
                        dimensions.Add(sc);
                    }
                    else
                    {
                        foreach (string equal in equalSplit)
                        {
                            dimensions.Add(equal);
                        }
                    }
                }
                else
                {
                    foreach (string space in spaceSplit)
                    {
                        string[] equalSplit = space.Split("=");
                        if (equalSplit.Length == 0)
                        {
                            dimensions.Add(space);
                        }
                        else
                        {
                            foreach (string equal in equalSplit)
                            {
                                dimensions.Add(equal);
                            }
                        }
                    }
                }
            }

            foreach (string dim in dimensions.Where(t => t.Trim() != string.Empty))
            {
                WhereClause = WhereClause + (WhereClause == string.Empty ? string.Empty : " and ") +
                    "( upper(ltrim(rtrim(description))) like '%=" +
                    dim.ToUpper().Trim() + "%' or" +
                    " upper(ltrim(rtrim(description))) like '%" +
                    dim.ToUpper().Trim() + "=%'" + ")";
            }

            RawSql = RawSql + (WhereClause == string.Empty ? WhereClause :
                " or (" + WhereClause + ")");

            var reader = ExecuteInlineQueryWithResultSet(
                Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var view = new DashCatalog
                {
                    SpanId = row.span_id,
                    RowDesc = row.description
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<DashCatalog> GetDashCatalog(string Topic)
        {
            List<DashCatalog> ReturnValue = new List<DashCatalog>();

            Topic = string.IsNullOrEmpty(Topic) ? string.Empty : Topic.ToUpper().Trim();
            string RawSql = "select b.name datasource_name,c.name asset_name,d.name metric_name,coalesce(d.short_name,'') metric_short_name,d.description metric_description,d.frequency metric_frequency, " +
                            "d.lag metric_lag,e.description span_name,a.asset_id,a.metric_id,a.datasource_id,a.span_id,h.name periodtype_name,i.name stat_name,i.description stat_description,i.stat_id,h.period_type_id " +
                            "from " +
                            "core_catalog.metric_span_map a " +
                            "join core_catalog.datasource b on (a.datasource_id = b.datasource_id) " +
                            "join core_catalog.asset c on (a.asset_id = c.asset_id) " +
                            "join core_catalog.metric d on (a.metric_id = d.metric_id) " +
                            "join core_catalog.span e on (a.span_id = e.span_id) " +
                            "join core_catalog.breakdown f on (a.asset_id = f.asset_id and a.datasource_id = f.datasource_id and a.metric_id = f.metric_id) " +
                            "join core_catalog.breakdown_period_stat_map g on (f.breakdown_id = g.breakdown_id) " +
                            "join core_catalog.period_type h on (g.period_type_id = h.period_type_id) " +
                            "join core_catalog.stat i on (g.stat_id = i.stat_id) " +
                            (string.IsNullOrEmpty(Topic) ? string.Empty :
                                "where ltrim(rtrim(upper(c.name))) =  '" + Topic + "'") +
                            " order by b.name, c.name, d.name, e.description;";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.AltDataConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var view = new DashCatalog
                {
                    DataSourceId = row.datasource_id,
                    AssetId = row.asset_id,
                    MetricId = row.metric_id,
                    SpanId = row.span_id,
                    StatId = row.stat_id,
                    PeriodTypeId = row.period_type_id,
                    DataSourceName = row.datasource_name,
                    AssetName = row.asset_name,
                    MetricName = row.metric_name,
                    MetricShortName = row.metric_short_name,//(row.BusDate is System.DBNull ? DateTime.MinValue : Convert.ToDateTime(row.BusDate)),
                    MetricDesc = row.metric_description,
                    MetricFrequency = row.metric_frequency,
                    MetricLag = row.metric_lag,
                    SpanName = row.span_name,
                    StatName = row.stat_name,
                    StatDesc = row.stat_description,
                    PeriodTypeName = row.periodtype_name,
                    RowDesc = row.asset_name + " " + row.datasource_name + " " + row.metric_name +
                                    " " + Convert.ToString(row.span_name).Replace(";", " ") +
                                    " " + row.periodtype_name + " " + row.stat_name
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<AnalystIdea> GetStaleAnalystIdeas(string Username)
        {
            List<AnalystIdea> ReturnValue = new List<AnalystIdea>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username }
                };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetStaleAnalystIdeas", parameters);

            foreach (dynamic row in reader)
            {
                var rec = new AnalystIdea
                {
                    IdeaAge = (row.IdeaAge is System.DBNull ? null : row.IdeaAge),
                    AnalystCode = row.AnalystCode,
                    AnalystDesc = row.AnalystDesc,
                    Symbol = row.Symbol,
                    IdeaAbstractShortTermViewCode = row.IdeaAbstractShortTermViewCode,
                    IdeaAbstractShortTermViewDesc = row.IdeaAbstractShortTermViewDesc,
                    PortfolioTimestamp = (row.BusDate is System.DBNull ? null : row.BusDate)
                };
                ReturnValue.Add(rec);
            }

            return ReturnValue;
        }

        public static Int64 InsertDashQuery(string QueryName, string QueryDesc,
            string Query, string Aesthetic, int UserId)
        {
            Int64 ReturnValue = 0;

            string RawSql = "insert into core_catalog.dash_query (queryname,querydesc,aesthetic,user_id)" +
                                "values ('" + QueryName + "','" + QueryDesc + "','" + Aesthetic + "'," + UserId.ToString() +
                                ") RETURNING dash_query_id;";

            ReturnValue = Convert.ToInt64(PostgresExecuteStoredProcedureWithReturnValue
                (Startup.AltDataConnectionString, RawSql));

            return ReturnValue;
        }

        public static void InsertDashViewQuery(Int64 DashViewId, Int64 DashQueryId,
            int DisplayOrder, bool IsActive)
        {
            string RawSql = "insert into core_catalog.dash_view_query (dash_view_id,dash_query_id,display_order,is_active)" +
                                "values (" + DashViewId.ToString() + "," + DashQueryId.ToString() + "," + DisplayOrder + ",true" +
                                ") RETURNING dash_view_query_id;";

            Int64 ReturnValue = Convert.ToInt64(PostgresExecuteStoredProcedureWithReturnValue
                (Startup.AltDataConnectionString, RawSql));
        }

        public static Int64 GetDashAssetId(string Topic)
        {
            Int64 ReturnValue = 0;

            Topic = string.IsNullOrEmpty(Topic) ? string.Empty : Topic.ToUpper().Trim();
            string RawSql = "select asset_id from data.asset where upper(ltrim(rtrim(name))) = '" +
                            Topic.Trim().ToUpper() + "';";

            object temp = ExecuteInlineQueryWithReturnValue(Startup.HoloceneDatabaseConnectionString,
                RawSql);
            if (temp != null)
            {
                ReturnValue = Convert.ToInt64(temp);
            }

            return ReturnValue;
        }

        public static Int64 UpsertDashView(Int64 DashViewId,
            string DashViewName, string DashViewDesc, string Asset, string Username)
        {
            Int64 ReturnValue = 0;

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashViewId", SqlDbType = SqlDbType.NVarChar, Value = DashViewId },
                    new SqlParameter() {ParameterName = "@DashViewName", SqlDbType = SqlDbType.NVarChar, Value = DashViewName },
                    new SqlParameter() {ParameterName = "@DashViewDesc", SqlDbType = SqlDbType.NVarChar, Value = DashViewDesc },
                    new SqlParameter() {ParameterName = "@Asset", SqlDbType = SqlDbType.NVarChar, Value = Asset },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username }
                };

            ReturnValue = (long)ExecuteStoredProcedureWithReturnValueO(
                Startup.HoloceneDatabaseConnectionString,
                "data.UpsertDashView", parameters);

            return ReturnValue;
        }

        public static Int64 UpsertDashChart(Int64 DashChartId, string DashChartTitle, string Aesthetic, string Username)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (DashChartId == 0)
            {
                RawSql = "insert into data.DashChart " +
                         "(Title,Aesthetic,IsActive,ModifiedBy) " +
                         " values ('" + DashChartTitle + "','" + Aesthetic + "',1,'" + Username + "'" +
                         ") SELECT SCOPE_IDENTITY();";
            }
            else
            {
                RawSql = "update data.DashChart " +
                         "set Title = '" + DashChartTitle + "'," +
                         "Aesthetic = '" + Aesthetic + "'," +
                         "ModifiedBy = '" + Username + "' " +
                         "where DashChartId = " + DashChartId.ToString() + "; select " + DashChartId.ToString() +
                         ";";
            }

            ReturnValue = Convert.ToInt64(ExecuteInlineQueryWithReturnValue(
                Startup.HoloceneDatabaseConnectionString, RawSql));

            return ReturnValue;
        }

        public static Int64 UpsertDashQuery(DashQuery Query, string Username)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (Query.DashQueryId == 0)
            {
                RawSql = "insert into data.DashQuery " +
                         "(QueryName,QueryDesc,Query,Aesthetic,SpanId,StatId,PeriodTypeId,IsActive,ModifiedBy,DisplayOrder) " +
                         " values ('','" + Query.QueryDesc + "','" + Query.Query + "','" + Query.Aesthetic
                         + "'," + Query.SpanId + "," + Query.StatId + "," + Query.PeriodTypeId +
                         ",1,'" + Username + "'," + Query.DisplayOrder.ToString() +
                         ") SELECT SCOPE_IDENTITY();";
            }
            else
            {
                RawSql = "update data.DashQuery " +
                         "set Query = '" + Query.Query + "'," +
                         "QueryDesc = '" + Query.QueryDesc + "'," +
                         "Aesthetic = '" + Query.Aesthetic + "'," +
                         "SpanId = " + Query.SpanId + "," +
                         "StatId = " + Query.StatId + "," +
                         "PeriodTypeId = " + Query.PeriodTypeId + "," +
                         "ModifiedBy = '" + Username + "' " +
                         "where DashQueryId = " + Query.DashQueryId.ToString() + "; select " + Query.DashQueryId.ToString() +
                         ";";
            }

            ReturnValue = Convert.ToInt64(ExecuteInlineQueryWithReturnValue
                (Startup.HoloceneDatabaseConnectionString, RawSql));

            return ReturnValue;
        }

        public static Int64 DeleteDashChartQuery(Int64 DashChartId)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (DashChartId != 0)
            {
                RawSql = "delete from data.DashChartQuery where DashChartId = " + DashChartId.ToString() + ";" +
                         " SELECT 1;";
            }

            ReturnValue = Convert.ToInt64(ExecuteInlineQueryWithReturnValue
                (Startup.HoloceneDatabaseConnectionString, RawSql));

            return ReturnValue;
        }

        public static Int64 UpsertDashChartQuery(Int64 DashChartId, Int64 DashQueryId, string Username, int DisplayOrder)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (DashChartId != 0 && DashQueryId != 0)
            {
                RawSql = "insert into data.DashChartQuery " +
                         "(DashChartId,DashQueryId,IsActive,ModifiedBy,DisplayOrder) " +
                         " values (" + DashChartId + "," + DashQueryId + ",1,'" + Username + "'," +
                         DisplayOrder.ToString() + ") SELECT SCOPE_IDENTITY();";
            }

            ReturnValue = Convert.ToInt64(ExecuteInlineQueryWithReturnValue
                (Startup.HoloceneDatabaseConnectionString, RawSql));

            return ReturnValue;
        }

        public static void UpdateDashViewChartSequence(Int64 DashViewChartId, int DisplayOrder, string Username)
        {
            string RawSql = string.Empty;

            if (DashViewChartId != 0)
            {
                RawSql = "update data.DashViewChart set " +
                    "DisplayOrder =  " + DisplayOrder + "," +
                    "ModifiedBy =  '" + Username + "' " +
                    "where DashViewChartId = " + DashViewChartId.ToString() +
                    ";select 1;";

                ExecuteInlineQueryWithReturnValue(
                    Startup.HoloceneDatabaseConnectionString, RawSql);
            }
        }

        public static void UpdateDashViewChartStatus(Int64 DashViewChartId, bool IsActive, string Username)
        {
            string RawSql = string.Empty;

            if (DashViewChartId != 0)
            {
                RawSql = "update data.DashViewChart set " +
                    "IsActive =  " + (IsActive == true ? 1 : 0) + "," +
                    "ModifiedBy =  '" + Username + "' " +
                    "where DashViewChartId = " + DashViewChartId.ToString() +
                    ";select 1;";

                ExecuteInlineQueryWithReturnValue(
                    Startup.HoloceneDatabaseConnectionString, RawSql);
            }
        }

        public static void UpdateDashViewAssetStatus(Int64 DashViewAssetId, bool IsActive, string Username)
        {
            string RawSql = string.Empty;

            if (DashViewAssetId != 0)
            {
                RawSql = "update data.DashViewAsset set " +
                    "IsActive =  " + (IsActive == true ? 1 : 0) + "," +
                    "ModifiedBy =  '" + Username + "' " +
                    "where DashViewAssetId = " + DashViewAssetId.ToString() +
                    ";select 1;";

                ExecuteInlineQueryWithReturnValue(
                    Startup.HoloceneDatabaseConnectionString, RawSql);
            }
        }

        public static void InsertDashViewChart(DashViewChart DashViewChart, string Username)
        {
            string RawSql = string.Empty;

            if (DashViewChart.DashViewId != 0 && DashViewChart.DashChartId != 0)
            {
                RawSql = "insert into data.DashViewChart" +
                    "(DashViewId,DashChartId,IsActive,DisplayOrder,ModifiedBy,ModifiedOn)" +
                    "select " + DashViewChart.DashViewId + "," +
                    DashViewChart.DashChartId + ",true,1000,'" +
                    Username + "',getdate()" +
                    ";select 1;";

                ExecuteInlineQueryWithReturnValue(
                    Startup.HoloceneDatabaseConnectionString, RawSql);
            }
        }

        public static Int64 UpsertDashViewChart(Int64 DashViewId,Int64 DashChartId, int DisplayOrder, string Username)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (DashViewId != 0 && DashChartId != 0)
            {
                List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashViewId", SqlDbType = SqlDbType.BigInt, Value = DashViewId },
                    new SqlParameter() {ParameterName = "@DashChartId", SqlDbType = SqlDbType.BigInt, Value = DashChartId },
                    new SqlParameter() {ParameterName = "@DisplayOrder", SqlDbType = SqlDbType.Int, Value = DisplayOrder },
                    new SqlParameter() {ParameterName = "@Username", SqlDbType = SqlDbType.NVarChar, Value = Username }
                    };

                ReturnValue = (long)ExecuteStoredProcedureWithReturnValueO(
                    Startup.HoloceneDatabaseConnectionString,
                    "data.UpsertDashViewChart", parameters);
            }

            return ReturnValue;
        }

        public static Int64 UpsertDashQueryCache(Int64 DashQueryId, string CacheData)
        {
            Int64 ReturnValue = 0;

            string RawSql = string.Empty;

            if (DashQueryId != 0)
            {
                RawSql = "delete from data.DashQueryCache where DashQueryId = " +
                         DashQueryId + ";";
                ExecuteInlineQueryWithReturnValue(Startup.HoloceneDatabaseConnectionString, RawSql);

                RawSql = "insert into data.DashQueryCache (DashQueryId,CacheData) " +
                         "values(" + DashQueryId + ",'" + CacheData + "');";
                ExecuteInlineQueryWithReturnValue(Startup.HoloceneDatabaseConnectionString, RawSql);
            }

            return ReturnValue;
        }

        public static DashQuery GetDashQuery(Int64 DashQueryId)
        {
            DashQuery ReturnValue = new DashQuery();

            string RawSql = string.Empty;
            RawSql = "select * from data.DashQuery (nolock) " +
                     "where DashQueryId = " + DashQueryId.ToString() + ";";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                ReturnValue = new DashQuery
                {
                    DashQueryId = row.DashQueryId,
                    Query = row.Query,
                    Aesthetic = row.Aesthetic,
                    QueryName = row.QueryName,
                    QueryDesc = row.QueryDesc,
                    SpanId = row.SpanId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.SpanId),
                    StatId = row.StatId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.StatId),
                    PeriodTypeId = row.PeriodTypeId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.PeriodTypeId),
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
            }

            return ReturnValue;
        }

        public static List<DashQuery> GetDashQueries()
        {
            List<DashQuery> ReturnValue = new List<DashQuery>();

            string RawSql = string.Empty;
            RawSql = "select * from data.DashQuery (nolock) where coalesce(IsActive,0) = 1;";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                ReturnValue.Add(new DashQuery
                {
                    DashQueryId = row.DashQueryId,
                    Query = row.Query,
                    Aesthetic = row.Aesthetic,
                    QueryName = row.QueryName,
                    QueryDesc = row.QueryDesc,
                    SpanId = row.SpanId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.SpanId),
                    StatId = row.StatId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.StatId),
                    PeriodTypeId = row.PeriodTypeId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.PeriodTypeId),
                    IsActive = Convert.ToBoolean(row.IsActive)
                });
            }

            return ReturnValue;
        }

        public static List<DashChart> GetDashViewChart(Int64 DashViewId)
        {
            List<DashChart> ReturnValue = new List<DashChart>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashViewId", SqlDbType = SqlDbType.NVarChar, Value = DashViewId }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashViewCharts", parameters);

            foreach (dynamic row in reader)
            {
                var chart = new DashChart
                {
                    DashViewChartId = row.DashViewChartId,
                    DashChartId = row.DashChartId,
                    ChartTitle = row.Title,
                    Aesthetic = row.Aesthetic,
                    DisplayOrder = Convert.ToInt32(row.DisplayOrder is System.DBNull ? 0 : row.DisplayOrder)
                };
                ReturnValue.Add(chart);
            }

            foreach (DashChart chart in ReturnValue)
            {
                chart.DashQuery = GetDashChartQuery(chart.DashChartId);
            }

            return ReturnValue;
        }

        public static List<DashQuery> GetDashChartQuery(Int64 DashChartId)
        {
            List<DashQuery> ReturnValue = new List<DashQuery>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@DashChartId", SqlDbType = SqlDbType.NVarChar, Value = DashChartId }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashChartQueries", parameters);

            foreach (dynamic row in reader)
            {
                var query = new DashQuery
                {
                    DashQueryId = row.DashQueryId,
                    QueryName = row.QueryName,
                    QueryDesc = row.QueryDesc,
                    Query = row.Query,
                    Aesthetic = row.Aesthetic,
                    SpanId = row.SpanId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.SpanId),
                    StatId = row.StatId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.StatId),
                    PeriodTypeId = row.PeriodTypeId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.PeriodTypeId),
                    CacheData = row.CacheData is System.DBNull ? string.Empty : row.CacheData
                };
                ReturnValue.Add(query);
            }

            return ReturnValue;
        }

        public static List<DashChart> GetDashCharts()
        {
            List<DashChart> ReturnValue = new List<DashChart>();

            string RawSql = "select * from data.DashChart (nolock)" +
                    "where coalesce(IsActive,0) = 1 order by Title;";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                var chart = new DashChart
                {
                    DashChartId = row.DashChartId,
                    ChartTitle = row.Title,
                    Aesthetic = row.Aesthetic is DBNull ? null : row.Aesthetic,
                    IsActive = Convert.ToBoolean(row.IsActive)
                };
                ReturnValue.Add(chart);
            }

            return ReturnValue;
        }

        public static DashChart GetDashChart(Int64 DashChartId)
        {
            DashChart ReturnValue = new DashChart();

            string RawSql = "select * from data.DashChart " +
                    "where DashChartId = " + DashChartId + ";";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);
            dynamic row = reader.First();
            ReturnValue = new DashChart
            {
                DashChartId = row.DashChartId,
                ChartTitle = row.Title,
                Aesthetic = row.Aesthetic,
                IsActive = Convert.ToBoolean(row.IsActive)
            };

            ReturnValue.DashQuery = GetDashChartQuery(DashChartId);

            return ReturnValue;
        }

        public static List<RecordHistory> GetRecordHistory(string Query)
        {
            List<RecordHistory> ReturnValue = new List<RecordHistory>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@Query", Query));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetRecordHistoryQuery", parameters);

            foreach (dynamic row in reader)
            {
                var record = new RecordHistory
                {
                    SpanId = row.SpanId,
                    StatId = row.StatId,
                    PeriodId = row.PeriodId,
                    PeriodTypeId = row.PeriodTypeId,
                    IsActual = (row.IsActual is System.DBNull ? (bool?)null : Convert.ToBoolean(row.IsActual)),
                    Value = (row.Value is System.DBNull ? (double?)null : Convert.ToDouble(row.Value)),
                    PeriodName = row.Period,
                    PeriodTypeName = row.PeriodType,
                    StatName = row.StatName,
                    StatDesc = row.StatDesc,
                    RowDesc = row.RowDesc,
                    PeriodBegin = Convert.ToDateTime(row.PeriodBegin),
                    PeriodEnd = Convert.ToDateTime(row.PeriodEnd),
                    ModifiedOn = (row.ModifiedOn is System.DBNull ? (DateTime?)null : Convert.ToDateTime(row.ModifiedOn))
                };

                ReturnValue.Add(record);
            }

            return ReturnValue;
        }

        public static void InsertDashQueryCache(Int64 DashQueryId,
            string CacheData)
        {
            string RawSql = "insert into core_catalog.dash_query_cache (dash_query_id,cache_data)" +
                                "values (" + DashQueryId.ToString() + ",'" + DashQueryId.ToString() + "'" +
                                ") RETURNING dash_query_cache_id;";

            Int64 ReturnValue = Convert.ToInt64(PostgresExecuteStoredProcedureWithReturnValue
                (Startup.AltDataConnectionString, RawSql));
        }

        public static IEnumerable<StressValue> GetStressValues(string StressType)
        {
            List<StressValue> ReturnValue = new List<StressValue>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@StressType", StressType));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetStressValues", parameters);

            foreach (dynamic row in reader)
            {
                var val = new StressValue
                {
                    StressValueId = row.StressValueId,
                    Code = row.Code,
                    Desc = row.Desc,
                    Tooltip = row.Tooltip,
                    DisplayOrder = row.DisplayOrder
                };

                ReturnValue.Add(val);
            }

            return ReturnValue.AsEnumerable();
        }

        public static IEnumerable<StressScore> GetStressScores(string StressType, string Symbol)
        {
            List<StressScore> ReturnValue = new List<StressScore>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@StressType", StressType));
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetStressScores", parameters);

            foreach (dynamic row in reader)
            {
                var val = new StressScore
                {
                    StressScoreId = row.StressScoreId,
                    StressTypeCode = row.StressTypeCode,
                    StressValueCode = row.StressValueCode,
                    StressValueTooltip = row.StressValueTooltip,
                    StressValueId = row.StressValueId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    Symbol = row.Symbol,
                    BusDate = Convert.ToDateTime(row.BusDate)
                };

                ReturnValue.Add(val);
            }

            return ReturnValue.AsEnumerable();
        }

        public static StressScore GetLatestStressScore(string StressType, string Symbol)
        {
            StressScore ReturnValue = new StressScore();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@StressType", StressType));
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "research.GetLatestStressScore", parameters);

            foreach (dynamic row in reader)
            {
                ReturnValue = new StressScore
                {
                    StressScoreId = row.StressScoreId,
                    StressTypeCode = row.StressTypeCode,
                    StressValueCode = row.StressValueCode,
                    StressValueId = row.StressValueId,
                    Username = row.Username,
                    DisplayName = row.DisplayName,
                    Symbol = row.Symbol,
                    BusDate = Convert.ToDateTime(row.BusDate)
                };
            }

            return ReturnValue;
        }

        public static void UpdateStressScore(string StressType, string Symbol, string Username, string StressScore)
        {
            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@StressType", StressType));
            parameters.Add(new SqlParameter("@Symbol", Symbol));
            parameters.Add(new SqlParameter("@Username", Username));
            parameters.Add(new SqlParameter("@StressScore", StressScore));
            ExecuteStoredProcedure(
                Startup.HoloceneDatabaseConnectionString,
                "research.UpdateStressScore", parameters);
        }

        public static List<MacroDash> GetMacroDash()
        {
            List<MacroDash> ReturnValue = new List<MacroDash>();

            IEnumerable<dynamic> reader = ExecuteInlineQueryWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "select * from data.MacroDash (nolock) where coalesce(IsActive,0) = 1 order by DisplayOrder");

            foreach (dynamic row in reader)
            {
                var val = new MacroDash
                {
                    MacroDashId = row.MacroDashId,
                    Name = row.Name,
                    Description = row.Description,
                    IsPartial = Convert.ToBoolean(row.IsPartial),
                    DisplayOrder = Convert.ToInt32(row.DisplayOrder),
                    URL = row.URL,
                    Height = Convert.ToInt32(row.Height),
                };

                ReturnValue.Add(val);
            }

            return ReturnValue;
        }

        public static List<RecordHistory> GetRecordHistory(Int64 SpanId, Int64 StatId, Int64 PeriodTypeId)
        {
            List<RecordHistory> ReturnValue = new List<RecordHistory>();

            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@SpanId", SpanId));
            parameters.Add(new SqlParameter("@StatId", StatId));
            parameters.Add(new SqlParameter("@PeriodTypeId", PeriodTypeId));
            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetRecordHistory", parameters);

            foreach (dynamic row in reader)
            {
                var record = new RecordHistory
                {
                    SpanId = row.SpanId,
                    StatId = row.StatId,
                    PeriodId = row.PeriodId,
                    PeriodTypeId = row.PeriodTypeId,
                    IsActual = (row.IsActual is System.DBNull ? (bool?)null : Convert.ToBoolean(row.IsActual)),
                    Value = (row.Value is System.DBNull ? (double?)null : Convert.ToDouble(row.Value)),
                    PeriodName = row.Period,
                    PeriodTypeName = row.PeriodType,
                    StatName = row.StatName,
                    StatDesc = row.StatDesc,
                    RowDesc = row.RowDesc,
                    PeriodBegin = Convert.ToDateTime(row.PeriodBegin),
                    PeriodEnd = Convert.ToDateTime(row.PeriodEnd),
                    ModifiedOn = (row.ModifiedOn is System.DBNull ? (DateTime?)null : Convert.ToDateTime(row.ModifiedOn))
                };

                ReturnValue.Add(record);
            }

            return ReturnValue;
        }

        public static bool CloneDashView(Int64 DashViewId, string Topic, string Username)
        {
            List<SqlParameter> parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter("@DashViewId", DashViewId));
            parameters.Add(new SqlParameter("@Asset", Topic));
            parameters.Add(new SqlParameter("@Username", Username));

            ExecuteStoredProcedureWithReturnValue(
                Startup.HoloceneDatabaseConnectionString,
                "data.CloneDashView", parameters);

            return true;
        }

        public static TCA GetTCAGrade(string Symbol, DateTime BusDate)
        {
            TCA ReturnValue = new TCA();

            string RawSql = "select * from mktdata.ubs_tcost where bbid = '" + Symbol + "' and date(ts) = '" +
                    BusDate.ToString("yyyy-MM-dd") + "'";

            var reader = PostgresExecuteStoredProcedureWithResultSet
                (Startup.QRReportsDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                ReturnValue = new TCA
                {
                    Symbol = row.bbid,
                    BusDate = Convert.ToDateTime(row.ts),
                    Grade = row.grade,
                    Slippage = (row.slippage is System.DBNull ? null : Convert.ToDecimal(row.slippage)),
                    MidSpreadRatio = (row.mid_sprd_ratio is System.DBNull ? null : Convert.ToDecimal(row.mid_sprd_ratio)),
                    Vol = (row.vol is System.DBNull ? null : Convert.ToDecimal(row.vol)),
                    ADV = (row.adv is System.DBNull ? null : Convert.ToDecimal(row.adv))
                };
            }

            return ReturnValue;
        }

        public static BorrowRate GetBorrowRate(string Symbol)
        {
            BorrowRate ReturnValue = new BorrowRate();

            Symbol = Symbol.ToUpper();
            string RawSql = "select * from flex.BorrowRateView (nolock) where " +
                "BusDate = (select max(BusDate) from flex.BorrowRateView (nolock) where upper(Symbol) = '" + Symbol + "') and Symbol = '" + Symbol + "'";

            var reader = ExecuteInlineQueryWithResultSet
                (Startup.HoloceneDatabaseConnectionString, RawSql);

            foreach (dynamic row in reader)
            {
                ReturnValue = new BorrowRate
                {
                    Symbol = row.Symbol,
                    BusDate = Convert.ToDateTime(row.BusDate),
                    Fees = (row.BorrowRate is System.DBNull ? null : Convert.ToDecimal(row.BorrowRate)),
                    LocatedPercent = (row.LocatedPercent is System.DBNull ? null : Convert.ToDecimal(row.LocatedPercent)),
                    RequestedQuantity = (row.RequestedQuantity is System.DBNull ? null : Convert.ToDecimal(row.RequestedQuantity)),
                    LocatedQuantity = (row.LocatedQuantity is System.DBNull ? null : Convert.ToDecimal(row.LocatedQuantity))
                };
            }

            return ReturnValue;
        }

        public static List<DashAsset> GetDashAssets()
        {
            List<DashAsset> ReturnValue = new List<DashAsset>();

            var reader = ExecuteStoredProcedureWithResultSet(
                    Startup.HoloceneDatabaseConnectionString,
                    "data.GetDashAssets", null);

            foreach (dynamic row in reader)
            {
                var view = new DashAsset
                {
                    DashAssetId = row.DashAssetId,
                    AssetName = row.Name,
                    AssetDesc = row.Description,
                    IsActive = Convert.ToBoolean((row.IsActive is System.DBNull ? false : row.IsActive)),
                    UserId = (row.UserId is System.DBNull ? null : row.UserId)
                };

                ReturnValue.Add(view);
            }

            return ReturnValue;
        }

        public static List<CrmNote> GetCrmNotes(string Symbol)
        {
            List<CrmNote> ReturnValue = new List<CrmNote>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol } };

            IEnumerable<dynamic> reader = ExecuteStoredProcedureWithResultSet(Startup.HoloceneDatabaseConnectionString,
                "crm.GetNotes", parameters);

            foreach (dynamic row in reader)
            {
                var note = new CrmNote
                {
                    NoteId = row.NoteId,
                    Note = row.Note,
                    Owner = row.Owner,
                    OwnerId = row.OwnerId,
                    EventDate = Convert.ToDateTime(row.EventDate),
                    NoteType = row.NoteType,
                    Subject = row.Subject,
                    Symbol = row.Symbol,
                    ExpertNetwork = row.ExpertNetwork,
                    VsExpectations = row.VsExpectations,
                    Audience = row.Audience,
                    MeetingType = row.MeetingType,
                    ToneOfTheCall = row.ToneOfTheCall,
                    StockImplication = row.StockImplication,
                    TradeRecommendation = row.TradeRecommendation,
                    QualityOfInteraction = row.QualityOfInteraction,
                    ModifiedOn = Convert.ToDateTime(row.ModifiedOn)
                };
                ReturnValue.Add(note);
            }

            return ReturnValue;
        }

        public static string GetDashChartDefaultLayout()
        {
            string ReturnValue = string.Empty;

            //select * from data.dashchart (nolock) where Title = 'default_layout'
            Object rv = ExecuteInlineQueryWithReturnValue(
                Startup.HoloceneDatabaseConnectionString, "select aesthetic from data.dashchart (nolock) where Title = 'default_layout'");

            if (rv != null)
            {
                ReturnValue = rv.ToString();
            }

            return ReturnValue;
        }
        public static List<DashChart> GetDashViewChartNew(int DashViewId)
        {
            List<DashChart> result = new List<DashChart>();           
            List<SqlParameter> parameters = new List<SqlParameter>() {
                new SqlParameter() {ParameterName = "@DashViewId", SqlDbType = SqlDbType.NVarChar, Value = DashViewId }
            };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "data.GetDashViewCharts", parameters);

            foreach (dynamic row in reader)
            {
                var chart = new DashChart
                {
                    DashViewChartId = row.DashViewChartId,
                    DashChartId = row.DashChartId,
                    ChartTitle = row.Title,
                    Aesthetic = row.Aesthetic is DBNull ? null : row.Aesthetic,
                    DisplayOrder = Convert.ToInt32(row.DisplayOrder is DBNull ? 0 : row.DisplayOrder),
                    DisplayWidth = row.DisplayWidth is DBNull ? 3 : row.DisplayWidth,
                    DisplayHeight = row.DisplayHeight is DBNull ? 4 : row.DisplayHeight,
                    ChartUrl = row.ChartUrl is DBNull ? null : row.ChartUrl,
                    ChartHtml = row.ChartHtml is DBNull ? null : row.ChartHtml,
                    UpdateMethod = row.UpdateMethod is DBNull ? null : Int32.Parse(row.UpdateMethod)  
                };
                result.Add(chart);
            }

            Parallel.ForEach(result, chart =>
            {
                if (string.IsNullOrEmpty(chart.ChartUrl) && string.IsNullOrEmpty(chart.ChartHtml))
                {
                    chart.DashQuery = GetDashChartQueryNew(chart.DashChartId);
                }
            });      

            return result;
        }

        public static List<DashQuery> GetDashChartQueryNew(Int64 DashChartId)
        {
            List<DashQuery> ReturnValue = new List<DashQuery>();

            string sql = $@"
                        select
                            a.DashChartId,
                            a.DashQueryId,
                            a.DashChartQueryId,
                            b.QueryName,
                            b.QueryDesc,
                            b.Query,
                            b.SpanId,
                            b.StatId,
                            b.PeriodTypeId,
                            coalesce(a.Aesthetic,b.Aesthetic) Aesthetic,        
                            coalesce(a.Aesthetic_New,b.Aesthetic_New,'') AestheticNew,
                            coalesce(a.Xfield,'') Xfield,
                            coalesce(a.Yfield,'') Yfield,
                            coalesce(a.Name,'') [Name],
                            coalesce(a.CustomData,'') CustomData,
                            c.ModifiedOn
                        from 
                            data.DashChartQuery (nolock) a
                            join data.DashQuery (nolock) b on (a.DashQueryId = b.DashQueryId)
                            left join data.DashQueryCache (nolock) c on 
                                (b.DashQueryId = c.DashQueryId)
                        where
                            coalesce(a.IsActive,0) = 1 and
                            coalesce(b.IsActive,0) = 1 and
                            coalesce({DashChartId},0) <> 0 and
                            a.DashChartId = {DashChartId}
                        order by
                            a.DisplayOrder";

            var reader = ExecuteInlineQueryWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                sql);

            foreach (dynamic row in reader)
            {
                var query = new DashQuery
                {
                    DashQueryId = row.DashQueryId,
                    QueryName = row.QueryName is System.DBNull ? "" : row.QueryName,
                    QueryDesc = row.QueryDesc is System.DBNull ? "" : row.QueryDesc,
                    Query = row.Query is System.DBNull ? "" : row.Query,
                    Aesthetic = row.Aesthetic is System.DBNull ? "" : row.Aesthetic,
                    SpanId = row.SpanId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.SpanId),
                    StatId = row.StatId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.StatId),
                    PeriodTypeId = row.PeriodTypeId is System.DBNull ? (Int64?)null : Convert.ToInt64(row.PeriodTypeId),
                    AestheticNew = row.AestheticNew is System.DBNull ? "" : row.AestheticNew,
                    Xfield = row.Xfield is System.DBNull ? "" : row.Xfield,
                    Yfield = row.Yfield is System.DBNull ? "" : row.Yfield,
                    Name = row.Name is System.DBNull ? "" : row.Name,
                    CustomData = row.CustomData is System.DBNull ? "" : row.CustomData,
                    CacheDataModifiedOn = row.ModifiedOn is System.DBNull ? null : Convert.ToDateTime(row.ModifiedOn)
                };
                ReturnValue.Add(query);
            }

            if (!ReturnValue.Any())
            {
                return ReturnValue;
            }

            string distinctQueryIds = string.Join(",", ReturnValue.Select(x => x.DashQueryId.ToString()).Distinct());
            string sqlCache = $"SELECT DashQueryId, CacheData FROM [data].DashQueryCache (nolock) where DashQueryId in ({distinctQueryIds})";

            var readerCache = ExecuteInlineQueryWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                sqlCache);

            var queryIdToCache = readerCache.ToDictionary(x => (long)((dynamic)x).DashQueryId,
                x => (string)((dynamic)x).CacheData);

            foreach (var result in ReturnValue)
            {
                result.CacheData = queryIdToCache.GetValueOrDefault(result.DashQueryId);
            }

            return ReturnValue;
        }

        public static List<SecurityLevelAlert> GetSecurityAlerts(string Symbol)
        {
            List<SecurityLevelAlert> ReturnValue = new List<SecurityLevelAlert>();

            List<SqlParameter> parameters = new List<SqlParameter>() {
                    new SqlParameter() {ParameterName = "@Symbol", SqlDbType = SqlDbType.NVarChar, Value = Symbol }
                };

            var reader = ExecuteStoredProcedureWithResultSet(
                Startup.HoloceneDatabaseConnectionString,
                "alerts.GetSecurityAlerts", parameters);

            foreach (dynamic row in reader)
            {
                var query = new SecurityLevelAlert
                {
                    BusDate = Convert.ToDateTime(row.BusDate),
                    Symbol = row.Symbol,
                    AlertPriority = row.AlertPriority,
                    AlertType = row.AlertType,
                    Alert = row.Alert,
                    AlertTooltip = row.AlertTooltip,
                    FunctionName = row.FunctionName,
                    ModifiedBy = row.ModifiedBy,
                    ModifiedOn = Convert.ToDateTime(row.ModifiedOn)
                };
                ReturnValue.Add(query);
            }

            return ReturnValue;
        }
    }
}
