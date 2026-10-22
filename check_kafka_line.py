import json
from pathlib import Path

file_path = Path(
    r"D:\Programming\Data Eng ZUU Crew\dw-fundamentals-bootcamp\Z2D DWDF Capstone Project\DE01Capstone\stream\pulsetrack_conversions.log"
)

with file_path.open("r", encoding="utf-8") as file:
    for line_number, line in enumerate(file, start=1):
        if line_number == 451:
            print("Original record:")
            print(line)

            parts = line.rstrip("\r\n").split("|", 5)

            if len(parts) == 6:
                print("\nJSON payload:")
                print(parts[5])

                try:
                    json.loads(parts[5])
                    print("\nJSON is valid.")
                except json.JSONDecodeError as exc:
                    print("\nJSON is invalid.")
                    print(f"Error: {exc}")
                    print(f"Position: {exc.pos}")
                    print(
                        "Context:",
                        repr(parts[5][
                            max(0, exc.pos - 50):exc.pos + 50
                        ])
                    )
            else:
                print(f"Unexpected number of fields: {len(parts)}")

            break
    else:
        print("The file contains fewer than 451 lines.")