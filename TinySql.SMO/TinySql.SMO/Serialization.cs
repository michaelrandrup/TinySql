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
                        PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.All
                    };
            }
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
