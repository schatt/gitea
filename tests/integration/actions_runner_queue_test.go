// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package integration

import (
	"net/http"
	"testing"

	actions_model "gitea.dev/models/actions"
	"gitea.dev/models/db"
	"gitea.dev/models/unittest"
	"gitea.dev/tests"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActionsRunnerQueueList(t *testing.T) {
	defer tests.PrepareTestEnv(t)()
	ctx := t.Context()

	require.NoError(t, db.DeleteAllRecords("action_runner"))
	require.NoError(t, db.DeleteAllRecords("action_task"))
	require.NoError(t, db.DeleteAllRecords("action_run_job"))
	require.NoError(t, db.DeleteAllRecords("action_run"))

	runner := &actions_model.ActionRunner{Name: "queue-global-runner", TokenHash: "queue-hash", UUID: "queue-uuid", AgentLabels: []string{"queue-label"}}
	require.NoError(t, actions_model.CreateRunner(ctx, runner))
	runner = unittest.AssertExistsAndLoadBean(t, &actions_model.ActionRunner{Name: "queue-global-runner"})

	run := &actions_model.ActionRun{
		Title: "queue-run", RepoID: 3, OwnerID: 3, WorkflowID: "test.yaml", Index: 98765,
		TriggerUserID: 1, Ref: "refs/heads/master", CommitSHA: "c2d72f548424103f01ee1dc02889c1e2bff816b0",
		Event: "push", TriggerEvent: "push", Status: actions_model.StatusRunning,
	}
	require.NoError(t, db.Insert(ctx, run))

	runningJob := &actions_model.ActionRunJob{
		RunID: run.ID, RepoID: 3, OwnerID: 3, CommitSHA: run.CommitSHA, Name: "queue-running-job",
		Attempt: 1, JobID: "queue-running-job", Status: actions_model.StatusRunning, RunsOn: []string{"queue-label"},
	}
	require.NoError(t, db.Insert(ctx, runningJob))

	task := &actions_model.ActionTask{
		JobID: runningJob.ID, RunnerID: runner.ID, Status: actions_model.StatusRunning,
		RepoID: 3, OwnerID: 3, CommitSHA: run.CommitSHA,
	}
	task.GenerateAndFillToken()
	require.NoError(t, db.Insert(ctx, task))
	runningJob.TaskID = task.ID
	_, err := db.GetEngine(ctx).ID(runningJob.ID).Cols("task_id").Update(runningJob)
	require.NoError(t, err)

	require.NoError(t, db.Insert(ctx, &actions_model.ActionRunJob{
		RunID: run.ID, RepoID: 3, OwnerID: 3, CommitSHA: run.CommitSHA, Name: "queue-waiting-job",
		Attempt: 1, JobID: "queue-waiting-job", Status: actions_model.StatusWaiting, RunsOn: []string{"queue-label"},
	}))

	// A site admin can read org3/repo3, so both pills show on the org runners page.
	admin := loginUser(t, "user1")
	body := admin.MakeRequest(t, NewRequest(t, "GET", "/org/org3/settings/actions/runners"), http.StatusOK).Body.String()
	assert.Contains(t, body, "org3/repo3 · queue-running-job")
	assert.Contains(t, body, "org3/repo3 1")

	// user2 cannot read private org3/repo3. The global runner is still listed
	// on repo1's runners page, but its repo3 pills must not leak.
	user2 := loginUser(t, "user2")
	body = user2.MakeRequest(t, NewRequest(t, "GET", "/user2/repo1/settings/actions/runners"), http.StatusOK).Body.String()
	assert.Contains(t, body, "queue-global-runner")
	assert.NotContains(t, body, "queue-running-job")
	assert.NotContains(t, body, "org3/repo3")
}
