using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TinySql.Serialization;

namespace TinySql.Metadata.Sql.CommandLine
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                WriteHelp();
                Environment.Exit(0);
            }
            ParseArgs();
            if (Command == Commands.Unknown)
            {
                WriteHelp();
                Environment.Exit(1);
            }
            if (string.IsNullOrEmpty(ConnectionString))
            {
                WriteHelp();
                Environment.Exit(1);
            }
            if (string.IsNullOrEmpty(OutputFile))
            {
                OutputFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "TinySql", Path.GetRandomFileName() + ".json");
            }

            if (Command == Commands.CreateMetadata)
            {
                Environment.Exit(CreateMetadata());
            }
            else if (Command == Commands.UpdateMetadata)
            {
                Environment.Exit(CreateMetadata(true));
            }
        }

        private static int CreateMetadata(bool Update = false)
        {
            ConsoleColor c = Console.ForegroundColor;
            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Connecting to " + ConnectionString);
                SqlMetadataDatabase db = SqlMetadataDatabase.FromConnection(ConnectionString, Update, File.Exists(OutputFile) ? OutputFile : null);
                Console.WriteLine("{0} metadata for {1} tables...",Update ? "Updating" : "Building", Tables == null ? "all" : Tables.Length.ToString());
                if (Update)
                {
                    db.FileName = OutputFile;
                }
                db.MetadataUpdateEvent += db_MetadataUpdateEvent;
                MetadataDatabase mdb = db.BuildMetadata(true, Tables,Update);
                Console.WriteLine("Done. Metadata contains {0} tables", mdb.Tables.Count);
                Console.WriteLine("Saving metadata in the file {0}", OutputFile);
                mdb.ToFile(OutputFile);
                return 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                return 1;
            }
            finally
            {
                Console.ForegroundColor = c;
            }


        }

        static void db_MetadataUpdateEvent(int PercentDone, string Message, DateTime timestamp)
        {
            Console.WriteLine("{0}%: {1} at {2}", PercentDone,Message, timestamp);
        }


        private static void WriteHelp()
        {
            Console.WriteLine("Command line parameters:");
            Console.WriteLine("(required) [update|create] Will either create or update metadata. if 'update' is specified, you must also specify the output parameter to point to an existing metadata file to be updated");
            Console.WriteLine("(required) [connection|con]:\"<sql connection string>\": The connection string to use");
            Console.WriteLine("(optional) [output|out]:\"<path to output file>\": Path to the file where the metadata is json serialized. Specify an existing file to update the file with modified metadata from the database");
            Console.WriteLine("(optional) [tables]:<table1,table2...table n>: Comma separated list of tables to generate metadata for");

        }



        private static Dictionary<string, string> commands = new Dictionary<string, string>();
        private static string ConnectionString = "";
        private static string OutputFile = "";
        private static string[] Tables;
        private enum Commands
        {
            Unknown,
            CreateMetadata,
            UpdateMetadata
        }
        private static Commands Command = Commands.Unknown;

        private static string Clean(string s)
        {
            return s.TrimStart('"').TrimEnd('"');

        }

        private static string[] SplitCommand(string cmd)
        {
            int i = cmd.IndexOf(':');
            if (i < 0)
            {
                return new string[] { cmd };
            }
            else
            {
                return new string[] { cmd.Substring(0, i), cmd.Substring(i + 1) };
            }

        }
        private static void ParseArgs()
        {
            string[] s = System.Environment.GetCommandLineArgs();
            for (int i = 1; i < s.Length; i++)
            {
                string[] cmd = SplitCommand(s[i]);
                if (cmd.Length == 2)
                {
                    if (cmd[0].Equals("connection", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("con", StringComparison.OrdinalIgnoreCase))
                    {
                        ConnectionString = Clean(cmd[1]);
                    }
                    else if (cmd[0].Equals("output", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("out", StringComparison.OrdinalIgnoreCase))
                    {
                        OutputFile = Clean(cmd[1]);
                    }
                    else if (cmd[0].Equals("tables", StringComparison.OrdinalIgnoreCase))
                    {
                        Tables = Clean(cmd[1]).Split(',');
                    }
                }
                else
                {
                    if (cmd[0].Equals("create", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.CreateMetadata;
                    }
                    else if (cmd[0].Equals("update", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.UpdateMetadata;
                    }
                }
            }
        }


    }
}
