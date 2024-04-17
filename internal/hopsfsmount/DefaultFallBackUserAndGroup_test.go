package hopsfsmount

import (
	"fmt"
	"strings"
	"testing"
)

func TestDefaultFallBackUerAndGroup(t *testing.T) {
	cases := []struct {
		Name          string
		FallBackUser  string
		FallBackGroup string
		Error         string
	}{
		{
			"default owner not provided",
			"",
			"blah",
			"fallBackOwner or fallBackGroup cannot be empty",
		},
		{
			"default group not provided",
			"blah",
			"",
			"fallBackOwner or fallBackGroup cannot be empty",
		},
		{
			"non existent default owner",
			"blah",
			"blah",
			"error looking up default user",
		},
		{
			"non existent default group",
			"root",
			"blah",
			"error looking up default group",
		},
		{
			"correct default user and group",
			"root",
			"root",
			"",
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s values: fallback user %s, fallback group %s", tc.Name, tc.FallBackUser, tc.FallBackGroup), func(t *testing.T) {
			// this call is required because otherwise flags panics
			err := validateFallBackUserAndGroup(tc.FallBackUser, tc.FallBackGroup)
			if tc.Name != "correct default user and group" && err == nil && strings.Contains(err.Error(), tc.Error) {
				t.Errorf("Expected error: %s, for test case: %s, got no error", tc.Name, tc.Name)
			} else if tc.Name != "correct default user and group" && err != nil && strings.Contains(err.Error(), tc.Error) {
				t.Errorf("Expected error: %s, for test case: %s, got error %v", tc.Name, tc.Name, err)
			}

			if tc.Name != "correct default user and group" && err != nil {
				t.Errorf("Error %v: , not expcted for test case %s", err, tc.Name)
			}
		})
	}
}
