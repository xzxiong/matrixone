// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

// Each 8192 rows of data corresponds to an S3 input unit
const rowsPerS3Input int64 = 8192

// Estimating S3input using the number of written lines, Maintained by @LeftHandCold
func EstimateS3Input(writtenRows int64) float64 {
	return float64(writtenRows) / float64(rowsPerS3Input)
}