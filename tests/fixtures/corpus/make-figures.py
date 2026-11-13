"""Draws the corpus figures (run by make-fixtures.sh): a soundings chart well above the
8 KiB figure threshold and a 16x16 icon well below it."""
from PIL import Image, ImageDraw

W, H = 640, 400
img = Image.new("RGB", (W, H), "white")
d = ImageDraw.Draw(img)
# Sky-to-sea gradient background: a photo-like figure, not a flat icon.
for y in range(H):
    d.line([(0, y), (W, y)], fill=(200 - y // 3, 220 - y // 4, 255 - y // 6))
    for x in range(0, W, 7):
        d.point((x + y % 7, y), fill=(180 - y // 4, 200 - y // 5, 240 - y // 7))
depths = [("Amberline", 7.4), ("Brackenwhite", 6.9), ("Cormorant Gap", 5.2), ("Dunlin Spit", 4.8)]
for i, (name, depth) in enumerate(depths):
    x = 60 + i * 145
    top = 40
    bottom = top + int(depth * 40)
    for y in range(top, bottom):
        d.line([(x, y), (x + 90, y)], fill=(20 + (y * 7) % 60, 60 + (y * 3) % 80, 120 + (y * 5) % 100))
    d.text((x, bottom + 8), name, fill="black")
    d.text((x + 20, top - 16), f"{depth} m", fill="black")
d.text((10, 8), "Channel soundings below chart datum", fill="black")
img.save("chart.png", optimize=True)

icon = Image.new("RGB", (16, 16), "white")
ImageDraw.Draw(icon).ellipse([2, 2, 13, 13], fill=(30, 90, 160))
icon.save("icon.png", optimize=True)
