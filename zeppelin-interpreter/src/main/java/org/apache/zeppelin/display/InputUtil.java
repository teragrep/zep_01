package org.apache.zeppelin.display;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.display.ui.*;
import org.apache.zeppelin.interpreter.xref.display.Input;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InputUtil {

    private InputUtil() {

    }

    // @TODO(zjffdu). Use gson's RuntimeTypeAdapterFactory and remove the old input form support
    // in future.
    public static final RuntimeTypeAdapterFactory TypeAdapterFactory =
        RuntimeTypeAdapterFactory.of(InputImpl.class, "type")
            .registerSubtype(TextBox.class, "TextBox")
            .registerSubtype(Select.class, "Select")
            .registerSubtype(CheckBox.class, "CheckBox")
            .registerSubtype(Password.class, "Password")
            .registerSubtype(OldInput.OldTextBox.class, "input")
            .registerSubtype(OldInput.OldSelect.class, "select")
            .registerSubtype(OldInput.OldCheckBox.class, "checkbox")
            .registerSubtype(OldInput.class, null);
    // Syntax of variables: ${TYPE:NAME=DEFAULT_VALUE1|DEFAULT_VALUE2|...,VALUE1|VALUE2|...}
    // Type is optional. Type may contain an optional argument with syntax: TYPE(ARG)
    // NAME and VALUEs may contain an optional display name with syntax: NAME(DISPLAY_NAME)
    // DEFAULT_VALUEs may not contain display name
    // Examples:  ${age}                              textbox form without default value
    //            ${age=3}                            textbox form with default value
    //            ${age(Age)=3}                       textbox form with display name and default value
    //            ${country=US(United States)|UK|JP}  select form with
    //            ${checkbox( or ):country(Country)=US|JP,US(United States)|UK|JP}
    //                                                checkbox form with " or " as delimiter: will be
    //                                                expanded to "US or JP"
    private static final Pattern VAR_PTN = Pattern.compile("([_])?[$][{]([^=}]*([=][^}]*)?)[}]");
    private static final Pattern VAR_NOTE_PTN =
        Pattern.compile("([_])?[$]{2}[{]([^=}]*([=][^}]*)?)[}]");
    private static final String DEFAULT_DELIMITER = ",";

    public static TextBox textbox(String name, String defaultValue) {
      return new TextBox(name, defaultValue);
    }

    public static Select select(String name, Object defaultValue, OptionInput.ParamOption[] options) {
      return new Select(name, defaultValue, options);
    }

    public static CheckBox checkbox(String name, Object[] defaultChecked, OptionInput.ParamOption[] options) {
      return new CheckBox(name, defaultChecked, options);
    }

    private static String[] getNameAndDisplayName(String str) {
      Pattern p = Pattern.compile("([^(]*)\\s*[(]([^)]*)[)]");
      Matcher m = p.matcher(str.trim());
      if (m == null || m.find() == false) {
        return null;
      }
      String[] ret = new String[2];
      ret[0] = m.group(1);
      ret[1] = m.group(2);
      return ret;
    }

    private static String[] getType(String str) {
      Pattern p = Pattern.compile("([^:()]*)\\s*([(][^()]*[)])?\\s*:(.*)");
      Matcher m = p.matcher(str.trim());
      if (m == null || m.find() == false) {
        return null;
      }
      String[] ret = new String[3];
      ret[0] = m.group(1).trim();
      if (m.group(2) != null) {
        ret[1] = m.group(2).trim().replaceAll("[()]", "");
      }
      ret[2] = m.group(3).trim();
      return ret;
    }

    private static InputImpl getInputForm(Matcher match) {
      String hiddenPart = match.group(1);
      boolean hidden = false;
      if ("_".equals(hiddenPart)) {
        hidden = true;
      }
      String m = match.group(2);

      String namePart;
      String valuePart;

      int p = m.indexOf('=');
      if (p > 0) {
        namePart = m.substring(0, p);
        valuePart = m.substring(p + 1);
      } else {
        namePart = m;
        valuePart = null;
      }


      String varName;
      String displayName = null;
      String type = null;
      String arg = null;
      Object defaultValue = null;
      OptionInput.ParamOption[] paramOptions = null;

      // get var name type
      String varNamePart;
      String[] typeArray = getType(namePart);
      if (typeArray != null) {
        type = typeArray[0];
        arg = typeArray[1];
        varNamePart = typeArray[2];
      } else {
        varNamePart = namePart;
      }

      // get var name and displayname
      String[] varNameArray = getNameAndDisplayName(varNamePart);
      if (varNameArray != null) {
        varName = varNameArray[0];
        displayName = varNameArray[1];
      } else {
        varName = varNamePart.trim();
      }

      // get defaultValue
      if (valuePart != null) {
        // find default value
        int optionP = valuePart.indexOf(",");
        if (optionP >= 0) { // option available
          defaultValue = valuePart.substring(0, optionP);
          if (type != null && type.equals("checkbox")) {
            // checkbox may contain multiple default checks
            defaultValue = splitPipe((String) defaultValue);
          }
          String optionPart = valuePart.substring(optionP + 1);
          String[] options = splitPipe(optionPart);

          paramOptions = new OptionInput.ParamOption[options.length];

          for (int i = 0; i < options.length; i++) {

            String[] optNameArray = getNameAndDisplayName(options[i]);
            if (optNameArray != null) {
              paramOptions[i] = new OptionInput.ParamOption(optNameArray[0], optNameArray[1]);
            } else {
              paramOptions[i] = new OptionInput.ParamOption(options[i], null);
            }
          }


        } else { // no option
          defaultValue = valuePart;
        }

      }

      InputImpl input = null;
      if (type == null) {
        if (paramOptions == null) {
          input = new TextBox(varName, (String) defaultValue);
        } else {
          input = new Select(varName, defaultValue, paramOptions);
        }
      } else if (type.equals("checkbox")) {
        input = new CheckBox(varName, (Object[]) defaultValue, paramOptions);
      } else if (type.equals("password")) {
        input = new Password(varName);
      } else {
        throw new RuntimeException("Could not recognize dynamic form with type: " + type);
      }
      input.setArgument(arg);
      if (!StringUtils.isBlank(displayName)) {
        // only set displayName when it is not empty (user explicitly specify it)
        // e.g. ${name(display_name)=value)
        input.setDisplayName(displayName);
      }
      input.setHidden(hidden);
      return input;
    }

    public static LinkedHashMap<String, Input> extractSimpleQueryForm(String script,
                                                                      boolean noteForm) {
      LinkedHashMap<String, Input> forms = new LinkedHashMap<>();
      if (script == null) {
        return forms;
      }
      String replaced = script;

      Pattern pattern = noteForm ? VAR_NOTE_PTN : VAR_PTN;
      Matcher match = pattern.matcher(replaced);
      while (match.find()) {
        int first = match.start();
        if (!noteForm && first > 0 && replaced.charAt(first - 1) == '$') {
          continue;
        }
        InputImpl form = getInputForm(match);
        forms.put(form.name, form);
      }

      forms.remove("pql");
      return forms;
    }

    public static String getSimpleQuery(Map<String, Object> params, String script, boolean noteForm) {
      String replaced = script;

      Pattern pattern = noteForm ? VAR_NOTE_PTN : VAR_PTN;

      Matcher match = pattern.matcher(replaced);
      while (match.find()) {
        int first = match.start();

        if (!noteForm && first > 0 && replaced.charAt(first - 1) == '$') {
          continue;
        }
        InputImpl input = getInputForm(match);
        Object value;
        if (params.containsKey(input.name)) {
          value = params.get(input.name);
        } else {
          value = input.getDefaultValue();
        }

        String expanded;
        if (value instanceof Object[] || value instanceof Collection) {  // multi-selection
          OptionInput optionInput = (OptionInput) input;
          String delimiter = input.argument;
          if (delimiter == null) {
            delimiter = DEFAULT_DELIMITER;
          }
          Collection<Object> checked = value instanceof Collection ? (Collection<Object>) value
              : Arrays.asList((Object[]) value);
          List<Object> validChecked = new LinkedList<>();
          for (Object o : checked) {
            // filter out obsolete checked values
            if (optionInput.getOptions() != null) {
              for (OptionInput.ParamOption option : optionInput.getOptions()) {
                if (option.getValue().equals(o)) {
                  validChecked.add(o);
                  break;
                }
              }
            }
          }
          if (validChecked.isEmpty()) {
            expanded = StringUtils.join(checked, delimiter);
          } else {
            params.put(input.name, validChecked);
            expanded = StringUtils.join(validChecked, delimiter);
          }
        } else {
          // single-selection
          expanded = StringUtils.defaultString((String) value, "");
        }
        replaced = match.replaceFirst(expanded);
        match = pattern.matcher(replaced);
      }

      return replaced;
    }

    public static String[] split(String str) {
      return str.split(";(?=([^\"']*\"[^\"']*\")*[^\"']*$)");

    }

    public static String[] splitPipe(String str) {
      return split(str, '|');
    }

    public static String[] split(String str, char split) {
      return split(str, new String[]{String.valueOf(split)}, false);
    }

    public static String[] split(String str, String[] splitters, boolean includeSplitter) {
      String escapeSeq = "\"',;${}";
      char escapeChar = '\\';

      String[] blockStart = new String[]{"\"", "'", "${", "N_(", "N_<"};
      String[] blockEnd = new String[]{"\"", "'", "}", "N_)", "N_>"};

      return split(str, escapeSeq, escapeChar, blockStart, blockEnd, splitters, includeSplitter);

    }

    public static String[] split(String str, String escapeSeq, char escapeChar, String[] blockStart,
                                 String[] blockEnd, String[] splitters, boolean includeSplitter) {

      List<String> splits = new ArrayList<>();

      StringBuilder curString = new StringBuilder();

      boolean escape = false; // true when escape char is found
      int lastEscapeOffset = -1;
      int blockStartPos = -1;
      List<Integer> blockStack = new LinkedList<>();

      for (int i = 0; i < str.length(); i++) {
        char c = str.charAt(i);

        // escape char detected
        if (c == escapeChar && escape == false) {
          escape = true;
          continue;
        }

        // escaped char comes
        if (escape == true) {
          if (escapeSeq.indexOf(c) < 0) {
            curString.append(escapeChar);
          }
          curString.append(c);
          escape = false;
          lastEscapeOffset = curString.length();
          continue;
        }

        if (blockStack.size() > 0) { // inside of block
          curString.append(c);
          // check multichar block
          boolean multicharBlockDetected = false;
          for (int b = 0; b < blockStart.length; b++) {
            if (blockStartPos >= 0
                && getBlockStr(blockStart[b]).compareTo(str.substring(blockStartPos, i)) == 0) {
              blockStack.remove(0);
              blockStack.add(0, b);
              multicharBlockDetected = true;
              break;
            }
          }

          if (multicharBlockDetected == true) {
            continue;
          }

          // check if current block is nestable
          if (isNestedBlock(blockStart[blockStack.get(0)]) == true) {
            // try to find nested block start

            if (curString.substring(lastEscapeOffset + 1).endsWith(
                getBlockStr(blockStart[blockStack.get(0)])) == true) {
              blockStack.add(0, blockStack.get(0)); // block is started
              blockStartPos = i;
              continue;
            }
          }

          // check if block is finishing
          if (curString.substring(lastEscapeOffset + 1).endsWith(
              getBlockStr(blockEnd[blockStack.get(0)]))) {
            // the block closer is one of the splitters (and not nested block)
            if (isNestedBlock(blockEnd[blockStack.get(0)]) == false) {
              for (String splitter : splitters) {
                if (splitter.compareTo(getBlockStr(blockEnd[blockStack.get(0)])) == 0) {
                  splits.add(curString.toString());
                  if (includeSplitter == true) {
                    splits.add(splitter);
                  }
                  curString.setLength(0);
                  lastEscapeOffset = -1;

                  break;
                }
              }
            }
            blockStartPos = -1;
            blockStack.remove(0);
            continue;
          }

        } else { // not in the block
          boolean splitted = false;
          for (String splitter : splitters) {
            // forward check for splitter
            int curentLenght = i + splitter.length();
            if (splitter.compareTo(str.substring(i, Math.min(curentLenght, str.length()))) == 0) {
              splits.add(curString.toString());
              if (includeSplitter == true) {
                splits.add(splitter);
              }
              curString.setLength(0);
              lastEscapeOffset = -1;
              i += splitter.length() - 1;
              splitted = true;
              break;
            }
          }
          if (splitted == true) {
            continue;
          }

          // add char to current string
          curString.append(c);

          // check if block is started
          for (int b = 0; b < blockStart.length; b++) {
            if (curString.substring(lastEscapeOffset + 1)
                .endsWith(getBlockStr(blockStart[b])) == true) {
              blockStack.add(0, b); // block is started
              blockStartPos = i;
              break;
            }
          }
        }
      }
      if (curString.length() > 0) {
        splits.add(curString.toString().trim());
      }
      return splits.toArray(new String[]{});

    }

    private static String getBlockStr(String blockDef) {
      if (blockDef.startsWith("N_")) {
        return blockDef.substring("N_".length());
      } else {
        return blockDef;
      }
    }

    private static boolean isNestedBlock(String blockDef) {
      if (blockDef.startsWith("N_")) {
        return true;
      } else {
        return false;
      }
    }

}
