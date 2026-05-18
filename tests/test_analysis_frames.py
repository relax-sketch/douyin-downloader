from pathlib import Path

from PIL import Image

from analysis.frames import GridBuilder


def test_grid_builder_creates_three_by_three_canvas(tmp_path):
    frame_paths = []
    colors = [
        (255, 0, 0),
        (0, 255, 0),
        (0, 0, 255),
        (255, 255, 0),
        (255, 0, 255),
        (0, 255, 255),
        (20, 20, 20),
        (100, 100, 100),
        (220, 220, 220),
    ]
    for index, color in enumerate(colors, start=1):
        path = tmp_path / f"frame_{index}.jpg"
        Image.new("RGB", (10, 20), color=color).save(path)
        frame_paths.append(path)

    output = tmp_path / "grid.jpg"
    GridBuilder().build(frame_paths, output)

    with Image.open(output) as image:
        assert image.size == (30, 60)


def test_grid_builder_fits_canvas_to_target_max_side_when_one_side_exceeds_threshold(tmp_path):
    path = tmp_path / "large_frame.jpg"
    Image.new("RGB", (8000, 400), color=(10, 20, 30)).save(path)
    output = tmp_path / "large_grid.jpg"
    GridBuilder().build([path], output, rows=1, cols=1)

    with Image.open(output) as image:
        assert image.size == (4000, 200)
