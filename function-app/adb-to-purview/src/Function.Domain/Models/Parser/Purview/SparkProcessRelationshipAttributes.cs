// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Newtonsoft.Json;
namespace Function.Domain.Models.Purview
{
    public class SparkProcessRelationshipAttributes
    {
        [JsonProperty("application")]
        public RelationshipAttribute Application = new RelationshipAttribute();
    }

}
