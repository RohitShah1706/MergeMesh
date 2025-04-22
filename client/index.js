const p = require("@clack/prompts");
const { setTimeout } = require("node:timers/promises");
const color = require("picocolors");

let totalCorrect = 0;

class Question {
  constructor(question, answersArray, correctAnswerIndex) {
    this.question = question;
    this.answersArray = answersArray;
    this.correctAnswerIndex = correctAnswerIndex;
  }
}

const question1 = new Question(
  "1) In what year was PowerShell, a command-line shell and scripting language developed by Microsoft, first released?",
  ["1993", "1999", "2006", "2014"],
  2,
  "s",
  "a"
);

const question2 = new Question(
  "2) What was the display technology used in early computer terminals that employed glowing green characters on a black background?",
  [
    "Cathode Ray Tube (CRT)",
    "Vapotron Display",
    "Lumigenic Screen Array",
    "Phosphor-Enhanced Matrix (PEM) Display",
  ],
  0
);

const question3 = new Question(
  "3) What year did MacOS change their default shell from bash to zsh?",
  ["1990", "2013", "2019", "2022"],
  2
);

const allQuestions = [question1, question2, question3];

async function askQuestion(question, answers, correctAnswerIndex) {
  const options = [];
  answers.forEach((answer) => {
    options.push({ value: answer, label: answer });
  });

  const answer = await p.select({
    message: question,
    initialValue: "1",
    options: options,
  });

  const s = p.spinner();
  s.start();
  await setTimeout(1000);
  s.stop();

  if (answer == answers[correctAnswerIndex]) {
    totalCorrect++;
  }
}

async function main() {
  console.clear();

  await setTimeout(1000);

  p.intro(
    `${color.bgMagenta(
      color.black(
        " Welcome. Let us find out how much of a CLI expert you REALLY are. "
      )
    )}`
  );

  // Ask if the player is ready
  const readyToPlay = await p.select({
    message: "No cheating. 10 questions. Results at the end. Ready to play?",
    initialValue: "Yes",
    options: [
      { value: "Yes", label: "Yes" },
      { value: "No", label: "No" },
    ],
  });

  if (readyToPlay == "Yes") {
    // Begin trivia game
    for (const question of allQuestions) {
      await askQuestion(
        question.question,
        question.answersArray,
        question.correctAnswerIndex
      );
    }

    // Decide what ending screen to show based on how many questions user answered correctly
    p.outro(
      `${color.bgMagenta(
        color.black(`You got ${totalCorrect} questions correct!`)
      )}`
    );

    if (totalCorrect == 10) {
      const s = p.spinner();
      s.start("Generating secret message");
      await setTimeout(5000);
      s.stop();
      p.outro(
        `${color.bgMagenta(
          color.black(`The command line is a tool that is ripe for change. `)
        )}`
      );
    } else {
      const s = p.spinner();
      s.start();
      await setTimeout(3000);
      s.stop();
      p.outro(
        `${color.bgMagenta(
          color.black(
            `You need 10/10 correct to unlock the secret message. Try again.`
          )
        )}`
      );
    }
  } else {
    p.outro(`${color.bgMagenta(color.black(`Ok. Bye!`))}`);
  }
}

main().catch(console.error);
