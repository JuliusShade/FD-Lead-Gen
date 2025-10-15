# TableCellWithLinks Component Test Examples

This file contains examples of the types of content the TableCellWithLinks component can handle.

## Example Data Formats

### Primary Contact Examples
- `"Marketing Director: John Smith\nhttps://www.linkedin.com/in/johnsmith"`
- `"VP of Marketing: Sarah Johnson\nNo LinkedIn URL available"`
- `"Head of Partnerships: Mike Wilson\nhttps://linkedin.com/in/mikewilson"`

### Most Relevant Collab Examples
- `"TechReviewer (1,500,000 views) - Awesome Product Review https://youtube.com/watch?v=abc123"`
- `"GamerChannel (850,000 views) - Gaming Setup Tour | https://youtu.be/def456"`
- `"LifestyleBlogger (2,200,000 views) - My Morning Routine https://youtube.com/watch?v=ghi789"`

### Similar Creators Examples (Array format)
```json
[
  "CreatorOne (500,000 views) - Product Unboxing https://youtube.com/watch?v=jkl012",
  "CreatorTwo (750,000 views) - Setup Review | https://youtu.be/mno345",
  "CreatorThree (300,000 views) - Tech Tips https://youtube.com/watch?v=pqr678",
  "CreatorFour (1,100,000 views) - Ultimate Guide https://youtu.be/stu901"
]
```

## Expected Behavior

1. **URL Detection**: Automatically finds http:// and https:// URLs
2. **Link Icon**: Shows a small link icon (🔗) when URLs are detected
3. **Hover Popover**: Displays clickable links in a popover on hover
4. **Context Extraction**: Attempts to extract meaningful names from surrounding text
5. **Multiple URLs**: Handles multiple URLs in the same cell
6. **Accessibility**: Keyboard navigation and screen reader support
7. **Mobile Responsive**: Adjusts popover size and position for mobile devices

## Features

- ✅ Auto-detects URLs using regex
- ✅ Extracts context for better display names
- ✅ Shows clickable popover on hover
- ✅ Opens links in new tabs
- ✅ Handles arrays of strings (similar_creators)
- ✅ Mobile responsive popover positioning
- ✅ Keyboard navigation support
- ✅ Screen reader accessible with ARIA labels
- ✅ Prevents conflicts with existing tooltip system