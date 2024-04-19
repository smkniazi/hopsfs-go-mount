package hopsfsmount

import (
	"fmt"
	"os/user"
	"strings"
	"testing"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

func TestDefaultFallBackUerAndGroup(t *testing.T) {

	u, err := user.Current()
	if err != nil {
		t.Fatal("Couldn't determine current user")
	}

	user := u.Name
	group := u.Name
	gid := ugcache.LookupGid(user)
	uid := ugcache.LookupUId(group)

	cases := []struct {
		Name          string
		FallBackUser  string
		FallBackGroup string
		ErrorMsg      string
		UID           uint32
		GID           uint32
	}{
		{
			"default owner not provided",
			"",
			"blah",
			"fallBackOwner or fallBackGroup cannot be empty",
			0, 0,
		},
		{
			"default group not provided",
			"blah",
			"",
			"fallBackOwner or fallBackGroup cannot be empty",
			0, 0,
		},
		{
			"non existent default owner",
			"blah",
			"blah",
			"error looking up user",
			0, 0,
		},
		{
			"non existent default group",
			"root",
			"blah",
			"error looking up group",
			0, 0,
		},
		{
			"correct default user and group, root",
			"root",
			"root",
			"",
			0, 0,
		},
		{
			"correct default user and group, current user",
			user,
			group,
			"",
			uid, gid,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s values: fallback user %s, fallback group %s", tc.Name, tc.FallBackUser, tc.FallBackGroup), func(t *testing.T) {
			// this call is required because otherwise flags panics
			FallBackUser = tc.FallBackUser
			FallBackGroup = tc.FallBackGroup
			err := validateFallBackUserAndGroup()

			var msg = ""
			if err != nil {
				msg = err.Error()
			}

			if !strings.Contains(msg, tc.ErrorMsg) || ugcache.FallBackGID != tc.GID || ugcache.FallBackUID != tc.UID {
				t.Errorf("Test case \"%s\" failed.  Expected: %s, Got: %s. Fallback Test UID: %d Cache UID: %d,  Test GID: %d Cache GID: %d", tc.Name, tc.ErrorMsg, msg, tc.UID, ugcache.FallBackUID, tc.GID, ugcache.FallBackGID)

			}
		})
	}
}
