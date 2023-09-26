// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using Newtonsoft.Json;
namespace Function.Domain.Models.Purview
{
    public class SparkColumnLineageRelationshipAttributes
    {
        [JsonProperty("process")]
        public RelationshipAttribute Process = new RelationshipAttribute();

        [JsonProperty("inputs")]
        public List<InputOutput>? Inputs = new List<InputOutput>();

        [JsonProperty("outputs")]
        public List<InputOutput>? Outputs = new List<InputOutput>();
    }
}
