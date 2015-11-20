using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Converters;
using System;
using System.IO;
using System.Text;
using TinySql.Metadata;

namespace TinySql.Serialization
{
    public enum SerializerFormats : int
    {
        Json = 1,
        Bson = 2
    }
    public static class SerializationExtensions
    {
        private static Newtonsoft.Json.JsonSerializerSettings settings
        {
            get
            {
                return new Newtonsoft.Json.JsonSerializerSettings()
                {
                    PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                    ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
                    TypeNameAssemblyFormat = System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple,
                    TypeNameHandling = TypeNameHandling.Objects,
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    Culture = SqlBuilder.DefaultCulture

                };
            }
        }

        public static T FromJson<T>(string json)
        {
            using (StringReader sr = new StringReader(json))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                return serializer.Deserialize<T>(jr);
            }
        }

        public static byte[] ToBson<T>(T Object)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BsonWriter bw = new BsonWriter(ms))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                serializer.Serialize(bw, Object, typeof(T));
                return ms.ToArray();
            }
        }

        public static string ToJson<T>(T Object, bool FormatOutput = false)
        {
            StringBuilder sb = new StringBuilder();
            using (StringWriter sw = new StringWriter(sb))
            using (JsonWriter jw = new JsonTextWriter(sw))
            {
                jw.Formatting = FormatOutput ? Formatting.Indented : Formatting.None;
                JsonSerializer serializer = JsonSerializer.Create(settings);
                serializer.Serialize(jw, Object);
                sw.Flush();
            }
            return sb.ToString();
        }

        public static void ToFile<T>(T Object, string FileName, bool CreateDirectory = true, bool FormatOutput = false, SerializerFormats FileFormat = SerializerFormats.Json)
        {
            if (FileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                FileFormat = SerializerFormats.Json;
            }
            else if (FileName.EndsWith(".bson", StringComparison.OrdinalIgnoreCase))
            {
                FileFormat = SerializerFormats.Bson;
            }
            string Ext = FileFormat == SerializerFormats.Json ? "json" : "bson";
            if (CreateDirectory)
            {
                if (!Directory.Exists(Path.GetDirectoryName(FileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(FileName));
                }
            }
            if (!Path.GetExtension(FileName).ToLower().EndsWith(Ext))
            {
                FileName += Ext;
            }
            JsonSerializer serializer = JsonSerializer.Create(settings);
            if (FileFormat == SerializerFormats.Json)
            {
                using (FileStream fs = File.OpenWrite(FileName))
                using (StreamWriter sw = new StreamWriter(fs))
                using (JsonWriter jw = new JsonTextWriter(sw))
                {
                    jw.Formatting = FormatOutput ? Formatting.Indented : Formatting.None;
                    serializer.Serialize(jw, Object);
                }
            }
            else
            {
                using (MemoryStream ms = new MemoryStream())
                using (BsonWriter bw = new BsonWriter(ms))
                {
                    serializer.Serialize(bw, Object, typeof(T));
                    File.WriteAllBytes(FileName, ms.ToArray());
                }
            }
        }

        public static T FromFile<T>(string FileName, SerializerFormats? FileFormat = null)
        {
            if (FileFormat == null)
            {
                if (FileName.EndsWith(".bson", System.StringComparison.OrdinalIgnoreCase))
                {
                    FileFormat = SerializerFormats.Bson;
                }
                else if (FileName.EndsWith(".json", System.StringComparison.OrdinalIgnoreCase))
                {
                    FileFormat = SerializerFormats.Json;
                }
                else
                {
                    throw new ArgumentException("The file format cannot be infered from the file name. Set the FileFormat parameter", "FileFormat");
                }
            }
            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings()
            {
                PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.All
            };

            using (FileStream fs = File.OpenRead(FileName))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                using (StreamReader sr = new StreamReader(fs))
                {
                    if (FileFormat.Value == SerializerFormats.Json)
                    {
                        using (JsonTextReader jr = new JsonTextReader(sr))
                        {
                            return serializer.Deserialize<T>(jr);
                        }
                    }
                    else
                    {
                        using (BsonReader br = new BsonReader(fs))
                        {
                            return serializer.Deserialize<T>(br);
                        }
                    }
                }
            }
        }



        #region Metadata

        public static void ToFile(this MetadataDatabase Metadata, string FileName, bool CreateDirectory = true)
        {

            if (CreateDirectory)
            {
                if (!Directory.Exists(Path.GetDirectoryName(FileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(FileName));
                }
            }
            if (!Path.GetExtension(FileName).ToLower().EndsWith(".json"))
            {
                FileName += ".json";
            }
            using (FileStream fs = File.OpenWrite(FileName))
            using (StreamWriter sw = new StreamWriter(fs))
            using (JsonWriter jw = new JsonTextWriter(sw))
            {
                jw.Formatting = Formatting.None;
                JsonSerializer serializer = JsonSerializer.Create(settings);
                serializer.Serialize(jw, Metadata);
            }
        }
        public static MetadataDatabase FromFile(string FileName)
        {
            Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings()
            {
                PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.All
            };
            if (!Path.GetExtension(FileName).ToLower().EndsWith(".json"))
            {
                FileName += ".json";
            }
            using (FileStream fs = File.OpenRead(FileName))
            using (StreamReader sr = new StreamReader(fs))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                return serializer.Deserialize<MetadataDatabase>(jr);
            }
        }




        #endregion
    }
}
