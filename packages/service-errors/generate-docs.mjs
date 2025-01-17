#!/usr/bin/env node

/**
 * Usage:
 *    node generate-docs.mjs > docs.md
 */

import * as fs from 'node:fs';
import * as ts from 'typescript';

/**
 * Parse and generate Markdown docs for the ErrorCode enum in a given file.
 * - Groups error codes by preceding `//` section comments.
 * - Extracts JSDoc comments for each enum member.
 */
function generateMarkdownDocs(filePath) {
  // 1. Read the file content
  const sourceText = fs.readFileSync(filePath, 'utf-8');

  // 2. Create a SourceFile AST
  const sourceFile = ts.createSourceFile(filePath, sourceText, ts.ScriptTarget.ESNext, /*setParentNodes*/ true);

  // We will store data as follows:
  //   {
  //     "[section title]": [
  //       { name: "PSYNC_R0001", doc: "The doc comment..." },
  //       { name: "PSYNC_R2200", doc: "" }
  //     ],
  //     ...
  //   }
  //

  let mdOutput = '# ErrorCode Documentation\n\n';

  // 3. Recursively walk the AST looking for:
  //     - Single-line comment sections (// Some Section).
  //     - The "ErrorCode" enum declaration and its members.
  function visit(node) {
    // If this is an enum named "ErrorCode", record each member
    if (ts.isEnumDeclaration(node) && node.name.text === 'ErrorCode') {
      for (const member of node.members) {
        // Name of the enum member (e.g. "PSYNC_R0001")
        const name = member.name.getText(sourceFile);

        // Capture single-line leading comments so that each new // comment sets a "currentSection"
        const commentRanges = ts.getLeadingCommentRanges(sourceText, member.getFullStart());
        if (commentRanges) {
          for (const range of commentRanges) {
            // slice out the raw comment text
            const commentText = sourceText.slice(range.pos, range.end);
            if (commentText.trimStart().startsWith('//')) {
              // remove the leading slashes and trim
              const cleaned = commentText.replace(/^\/\/+/, '').trim();
              mdOutput += `${cleaned}\n`;
              mdOutput += `\n`;
            }
          }
        }

        // 4. Extract JSDoc for the member (if present)
        //
        //    There are a few ways to do this; one approach is to read the JSDoc comments
        //    in `ts.getJSDocTags(member)`, or use `ts.getJSDocCommentsAndTags(node)`.
        //
        //    For simplicity, we'll just look at the JSDoc comment text as a single
        //    block. If we need the specific @tags, we could parse that out too.
        let docComment = '';
        const jsDocNodes = ts.getJSDocCommentsAndTags(member);
        if (jsDocNodes.length > 0) {
          // Each node might have a comment portion
          // We'll just join them all as one string
          docComment = jsDocNodes
            .map((docNode) => {
              if (docNode.comment) {
                return docNode.comment.trim();
              }
              return '';
            })
            .filter(Boolean)
            .join('\n\n');
        }

        const docLines = docComment.split(/\r?\n/);

        mdOutput += `- **${name}**:\n`;
        for (const line of docLines) {
          mdOutput += `  ${line}\n`;
        }
        mdOutput += '\n';
      }
    }

    // Continue walking
    ts.forEachChild(node, visit);
  }

  // Kick off the AST walk
  visit(sourceFile);

  // 5. Generate Markdown output
  // We'll do a simple format, e.g.:
  // # ErrorCode Documentation
  //
  // ## PSYNC_Rxxxx: Sync rules issues
  // - **PSYNC_R0001**
  //   Catch-all sync rules parsing error, if no more specific error is available
  //
  // ## PSYNC_Sxxxx: Service issues
  // - **PSYNC_S0001**
  //   Internal assertion...
  // ...
  //

  return mdOutput;
}

const filePath = 'src/codes.ts';

const md = generateMarkdownDocs(filePath);
console.log(md);
