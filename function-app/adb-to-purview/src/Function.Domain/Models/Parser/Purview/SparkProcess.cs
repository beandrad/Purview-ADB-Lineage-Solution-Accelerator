// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using Newtonsoft.Json;
namespace Function.Domain.Models.Purview
{
    public class SparkProcess
    {
        [JsonProperty("typeName")]
        // public string TypeName = "spark_process_with_column_mapping";
        public string TypeName = "ProcessWithColumnMapping";
        [JsonProperty("attributes")]
        public SparkProcessAttributes Attributes = new SparkProcessAttributes();
        [JsonProperty("relationshipAttributes")]
        public SparkProcessRelationshipAttributes RelationshipAttributes = new SparkProcessRelationshipAttributes();
    }
}
