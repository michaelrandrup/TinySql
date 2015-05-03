using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace TinySql.Metadata
{
    public static class Serialization
    {
        private static Newtonsoft.Json.JsonSerializerSettings settings
        {
            get
            {
                return new Newtonsoft.Json.JsonSerializerSettings()
                    {
                        PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                         ReferenceLoopHandling = ReferenceLoopHandling.Serialize,

                         

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

        public static void ToFile(string FileName, MetadataDatabase Metadata)
        {

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

       
    }
}
