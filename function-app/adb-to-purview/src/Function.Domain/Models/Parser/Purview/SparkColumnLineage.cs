// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Newtonsoft.Json;
namespace Function.Domain.Models.Purview
{
    public class SparkColumnLineage
    {
        [JsonProperty("typeName")]
        public string TypeName = "spark_column_lineage";

        [JsonProperty("attributes")]
        public BaseAttributes Attributes = new BaseAttributes();
        
        [JsonProperty("relationshipAttributes")]
        public SparkColumnLineageRelationshipAttributes RelationshipAttributes = new SparkColumnLineageRelationshipAttributes();
    }
}
