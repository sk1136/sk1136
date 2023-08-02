sadssda
sad
sad
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
        }sdsd
dsa
sda
sda



hello there you man
