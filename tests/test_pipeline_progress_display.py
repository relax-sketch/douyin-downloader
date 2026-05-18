from types import SimpleNamespace

from cli.pipeline_progress_display import PipelineProgressDisplay


class _FakeProgress:
    def __init__(self):
        self.tasks = {}
        self._next_id = 1
        self.console = SimpleNamespace(print=lambda *_args, **_kwargs: None)

    def add_task(self, description, total, completed=0, detail="", **kwargs):
        task_id = self._next_id
        self._next_id += 1
        self.tasks[task_id] = {
            "description": description,
            "total": total,
            "completed": completed,
            "detail": detail,
        }
        self.tasks[task_id].update(kwargs)
        return task_id

    def update(self, task_id, **kwargs):
        self.tasks[task_id].update(kwargs)

    def advance(self, task_id, advance=1):
        self.tasks[task_id]["completed"] = self.tasks[task_id].get("completed", 0) + advance


class _FakeProgressContext:
    def __init__(self, progress):
        self.progress = progress
        self.exited = False

    def __enter__(self):
        return self.progress

    def __exit__(self, *_args):
        self.exited = True


def test_pipeline_progress_tracks_stage_and_overall_counts(monkeypatch):
    display = PipelineProgressDisplay()
    fake_progress = _FakeProgress()
    fake_ctx = _FakeProgressContext(fake_progress)
    monkeypatch.setattr(display, "create_progress", lambda: fake_ctx)

    display.start_pipeline("run-1", ("frames", "classify"))
    overall_task_id = display._overall_task_id
    assert overall_task_id is not None
    assert fake_progress.tasks[overall_task_id]["total"] == 2

    display.start_stage("frames", 2)
    frames_task_id = display._stage_task_ids["frames"]
    display.advance_stage_item("frames", "success", "a1")
    display.advance_stage_item("frames", "failed", "a2")
    display.finish_stage("frames")

    assert fake_progress.tasks[frames_task_id]["completed"] == 2
    assert "S:1 F:1 K:0" in fake_progress.tasks[frames_task_id]["description"]
    assert fake_progress.tasks[overall_task_id]["completed"] == 1

    display.start_stage("classify", 0)
    classify_task_id = display._stage_task_ids["classify"]
    assert fake_progress.tasks[classify_task_id]["completed"] == 1
    assert fake_progress.tasks[overall_task_id]["completed"] == 2


def test_pipeline_progress_formats_attempt_detail(monkeypatch):
    display = PipelineProgressDisplay()
    fake_progress = _FakeProgress()
    fake_ctx = _FakeProgressContext(fake_progress)
    monkeypatch.setattr(display, "create_progress", lambda: fake_ctx)

    display.start_pipeline("run-1", ("classify",))
    display.start_stage("classify", 5)
    task_id = display._stage_task_ids["classify"]

    display.update_stage_attempt(
        "classify",
        batch_index=2,
        total_batches=6,
        attempt=3,
        total_attempts=4,
        status="失败，等待重试",
        detail='回应：{"error":"busy"} · 5s 后第 4/4 次',
    )

    assert (
        fake_progress.tasks[task_id]["detail"]
        == '批次 2/6 · 尝试 3/4 · 失败，等待重试 · 回应：{"error":"busy"} · 5s 后第 4/4 次'
    )
