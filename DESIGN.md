# DESIGN.md

## 1. Brand Identity and Vibe

* **Keywords:** Reliable, Available, Fast
* **Aesthetic Principle:** Functional and high-performance, pairing smooth geometry with strong, definitive brutalist depths to communicate stability.

## 2. Color System

Agents must use plain CSS variables for color management. Do not use pre-built utility color classes unless explicitly mapped to these variables.

| Role | CSS Variable | Hex Code | Usage Notes |
| --- | --- | --- | --- |
| **Primary Brand** | `--tiger-orange` | `#f18805` | Primary buttons, active states, key brand highlights. |
| **Secondary Accent** | `--golden-orange` | `#f0a202` | Secondary highlights, warning states, hover variations. |
| **Destructive/Alert** | `--fiery-terracotta` | `#d95d39` | Error messages, destructive actions (delete/remove). |
| **Base/Text/Shadows** | `--space-indigo` | `#202c59` | Primary text, harsh brutalist drop-shadows, borders. |
| **Dark Accent** | `--espresso` | `#581f18` | Deep backgrounds, secondary dark accents. |

**Gradient Asset (Use Sparingly for High-Impact Backgrounds):**

```css
--brand-gradient: linear-gradient(135deg, #f0a202, #f18805, #d95d39, #202c59, #581f18);

```

## 3. Typography

* **Primary Font Family:** `Inter`, sans-serif. Used for all headings, body text, UI elements, and navigation.
* **Monospace Font:** System default monospace (e.g., `ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace`).
* **Constraint:** Monospace typography is strictly reserved for code execution blocks, inline code snippets, and raw data outputs. Never use monospace for general UI copy.

## 4. Layout and Spacing

* **Styling Methodology:** Plain CSS. Do not assume the presence of frameworks like Tailwind or Bootstrap.
* **Spacing System:** Strict 8px base increment scale.
* Extra Small: `8px`
* Small: `16px`
* Medium: `24px`
* Large: `32px`
* Extra Large: `48px`

## 5. Component Styling Rules

* **Borders & Radii:** Components (cards, buttons, inputs) must utilize rounded corners. Standard border-radius is `8px` or `12px` depending on component scale.
* **Shadows & Depth (Brutalist):** Elevation is communicated through brutalist, hard shadows rather than soft, blurred drop-shadows.
* *Standard Elevation Example:* `box-shadow: 4px 4px 0px var(--space-indigo); border: 2px solid var(--space-indigo);`

* **Interactive States:**
* **Hover:** Translate element slightly (e.g., `transform: translate(-2px, -2px)`) and increase the brutalist shadow offset to simulate raising up.
* **Active/Click:** Translate element down to baseline (e.g., `transform: translate(4px, 4px)`) and reduce shadow to `0px` to simulate a physical button press.
* **Disabled:** Reduce opacity to `0.5`, remove shadows, and set `cursor: not-allowed`.

## 6. Iconography

* **Icon Library:** Lucide Icons (or standard Material SVG icons) due to high ubiquity and clean, readable stroke lines.
* **Implementation:** Icons should maintain a consistent `2px` stroke width and scale harmoniously with the `Inter` text size they accompany.

## 7. Accessibility (a11y)

* **Contrast Targets:** Strict adherence to WCAG AA contrast ratios. Ensure primary text (`--space-indigo`) maintains high contrast against lighter backgrounds. If using `--tiger-orange` for buttons, ensure the button text is legible (use pure white `#ffffff` or `--space-indigo` depending on calculated contrast).
* **Focus Rings:** Keyboard navigation must be explicitly supported.
* *Focus State Rule:* Apply a highly visible, solid focus ring. Example: `outline: 3px solid var(--fiery-terracotta); outline-offset: 2px;`
* Never use `outline: none` without providing a custom, high-visibility focus alternative.

* **Aria Attributes:** Standard ARIA labels must be applied to all icon-only buttons and interactive non-text elements.
