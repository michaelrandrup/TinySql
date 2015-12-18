using TinySql.Metadata;

namespace TinySql
{
    public static class MetadataExtensions
    {

        public static SqlBuilder WithMetadata(this SqlBuilder builder, bool UseCache = true, string FileName = null)
        {
            SqlMetadataDatabase db = SqlMetadataDatabase.FromBuilder(builder, UseCache, FileName);
            builder.Metadata= db.BuildMetadata();
            return builder;
        }

    }
}
